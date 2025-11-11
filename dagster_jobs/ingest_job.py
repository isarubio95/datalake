import os
from datetime import datetime, timezone
from io import BytesIO
from zoneinfo import ZoneInfo
from typing import Iterable, List
import boto3
import botocore
import json
import mimetypes
from botocore.config import Config
import uuid
import pandas as pd
from dagster import (
    sensor, RunRequest, job, op, get_dagster_logger, OpExecutionContext, In, Failure,
    Out, Output
)
from dagster_dbt import DbtCliResource
from pyspark.sql import SparkSession
from zipfile import ZipFile, BadZipFile


# -------------------- Config --------------------
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/work/dbt")
BUCKET          = os.getenv("S3_BUCKET")
INGEST_PREFIX = os.getenv("INGEST_PREFIX")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")
TZ_EUROPE_MAD = ZoneInfo("Europe/Madrid")
_S3_CLIENT_CACHE: dict[str, boto3.client] = {}

dbt = DbtCliResource(project_dir="/work/dbt", profiles_dir=DBT_PROFILES_DIR)


# -------------------- Helpers --------------------
def _bucket_region(bucket: str) -> str:
    """
    Descubre la región real del bucket usando un cliente neutro.
    """
    probe = boto3.client("s3")
    resp = probe.get_bucket_location(Bucket=bucket)
    return resp.get("LocationConstraint") or "eu-west-1"

def make_s3():
    """
    Crea un cliente S3 de AWS en la región real del bucket.
    """
    if BUCKET in _S3_CLIENT_CACHE:
        return _S3_CLIENT_CACHE[BUCKET]

    region = _bucket_region(BUCKET)
    client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
    )
    _S3_CLIENT_CACHE[BUCKET] = client
    return client

def _local_parts(ts_utc: datetime) -> dict:
    ts_local = ts_utc.astimezone(TZ_EUROPE_MAD)
    return {
        "y":  ts_local.year,
        "m":  f"{ts_local.month:02d}",
        "d":  f"{ts_local.day:02d}",
        "ts": ts_local,
    }

def _list_objects(prefix: str) -> Iterable[dict]:
    """
    Lista objetos en AWS S3 con manejo de redirects por región.
    Si S3 devuelve PermanentRedirect/AuthorizationHeaderMalformed/IllegalLocationConstraint,
    se invalida la caché, se recrea el cliente en la región del bucket y se reintenta una vez.
    """
    s3 = make_s3()
    token = None
    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        try:
            resp = s3.list_objects_v2(**kwargs)
        except botocore.exceptions.ClientError as e:
            code = (e.response.get("Error", {}) or {}).get("Code")
            if code in {
                "PermanentRedirect",
                "AuthorizationHeaderMalformed",
                "IllegalLocationConstraintException",
                "301",
            }:
                # Recrear cliente en la región correcta y reintentar
                _S3_CLIENT_CACHE.pop(BUCKET, None)
                s3 = make_s3()
                resp = s3.list_objects_v2(**kwargs)
            else:
                raise

        for obj in resp.get("Contents", []) if "Contents" in resp else []:
            if not obj["Key"].endswith("/"):
                yield obj
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

def _read_tabular_from_bytes(name: str, data: bytes) -> pd.DataFrame:
    from io import BytesIO
    lname = name.lower()
    buf = BytesIO(data)
    if lname.endswith(".csv"):
        df = pd.read_csv(buf, sep=None, engine="python", encoding="utf-8-sig")
        df["source_sheet"] = pd.NA
    elif lname.endswith(".xlsx"):
        sheets = pd.read_excel(buf, engine="openpyxl", sheet_name=None)
        parts = [(sdf.assign(source_sheet=str(sname))) for sname, sdf in sheets.items()]
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    elif lname.endswith(".xls"):
        sheets = pd.read_excel(buf, engine="xlrd", sheet_name=None)
        parts = [(sdf.assign(source_sheet=str(sname))) for sname, sdf in sheets.items()]
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    elif lname.endswith(".json"):
        text = data.decode("utf-8-sig", errors="replace").strip()
        if not text:
            df = pd.DataFrame()
        else:
            try:
                # Caso 1: JSON cargable entero (array/objeto)
                obj = json.loads(text)
                if isinstance(obj, list):
                    df = pd.json_normalize(obj)
                elif isinstance(obj, dict):
                    for v in obj.values():
                        if isinstance(v, list):
                            df = pd.json_normalize(v)
                            break
                    else:
                        df = pd.json_normalize(obj)
                else:
                    raise ValueError("JSON no tabular")
            except Exception:
                # Caso 2: NDJSON
                lines = [ln for ln in text.splitlines() if ln.strip()]
                rows = []
                for ln in lines:
                    try:
                        rows.append(json.loads(ln))
                    except Exception:
                        raise Failure(f"JSON NDJSON no válido en {name}")
                df = pd.json_normalize(rows)
        df["source_sheet"] = pd.NA
    else:
        raise Failure(f"Unsupported inner file type for {name}")
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
    df["source_file"] = os.path.basename(name)
    return df


# -------------------- Ops --------------------
@op(ins={"key": In(default_value=None)})
def download_from_s3(context: OpExecutionContext, key: str | None) -> bytes:
    key = key or context.run.tags.get("s3_key")
    if not key:
        raise Failure("download_from_s3: falta 'key' (tag 's3_key' no presente).")
    s3 = make_s3()
    buf = BytesIO()
    s3.download_fileobj(Bucket=BUCKET, Key=key, Fileobj=buf)
    buf.seek(0)
    get_dagster_logger().info(f"Downloaded {key} ({buf.getbuffer().nbytes} bytes)")
    return buf.getvalue()

@op(ins={"payload": In(), "key": In(default_value=None)})
def validate_file(context: OpExecutionContext, payload: bytes, key: str | None) -> str:
    key = key or context.run.tags.get("s3_key") or ""
    k = key.lower()
    if k.endswith(".zip"):
        return "zip"
    raise Failure(f"The file {key} is not an zip file (.zip)")

@op(
    ins={"payload": In(), "key": In(default_value=None)},
    out=Out(str, description="La S3 key del 'datos.csv' generado.")
)
def process_zip_and_upload(context: OpExecutionContext, payload: bytes, key: str | None) -> str:
    """
    Descomprime, procesa 'intrinsics.json' y 'results.json' en memoria,
    genera 'datos.csv' y sube todo el contenido descomprimido + 'datos.csv' a S3.
    """
    logger = get_dagster_logger()
    key = key or context.run.tags.get("s3_key") or ""

    if not key.lower().endswith(".zip"):
        logger.info(f"process_zip_and_upload: {key} no es .zip, no se hace nada.")
        return ""

    try:
        zf = ZipFile(BytesIO(payload))
    except BadZipFile:
        raise Failure("ZIP corrupto o no válido")

    s3 = make_s3()
    created: list[str] = []
    
    # Datos que se extraerán del ZIP
    intrinsics_data: bytes | None = None
    results_data: bytes | None = None
    files_to_upload: list[tuple[str, bytes, str]] = [] # (dest_key, data, content_type)

    # Prefijo fecha 
    lm_iso = context.run.tags.get("s3_last_modified")
    ts_utc = datetime.fromisoformat(lm_iso) if lm_iso else datetime.now(timezone.utc)
    ts_local = ts_utc.astimezone(TZ_EUROPE_MAD)

    # Usa el código del folder del ZIP + UID(6)
    zip_stem = os.path.splitext(os.path.basename(key))[0]
    uid6 = uuid.uuid4().hex[:6]
    job_uid = f"{zip_stem}_{uid6}"
    
    # Prefijo en nuevo subnivel único
    prefix = f"data/{ts_local.year}/{ts_local.month:02d}/{ts_local.day:02d}/{job_uid}"
    
    # --- 1. Recorremos los archivos del ZIP (Solo lectura) ---
    for zi in zf.infolist():
        name = zi.filename
        if name.endswith("/") or "__MACOSX" in name or os.path.basename(name).startswith("~$"):
            continue
        
        with zf.open(zi, "r") as fp:
            data = fp.read()
            if not data: # Ignorar archivos vacíos
                continue
        
        filename = os.path.basename(name)
        dest_key = f"{prefix}/{filename}"
        
        # Guardar datos para subida posterior
        ctype, _ = mimetypes.guess_type(filename)
        ctype = ctype or "application/octet-stream"
        files_to_upload.append((dest_key, data, ctype))

        # Capturar intrinsics y results en memoria
        base_lower = filename.lower()
        if base_lower == "intrinsics.json":
            intrinsics_data = data
        elif base_lower == "results.json":
            results_data = data

    if not files_to_upload:
        raise Failure("El ZIP no contenía archivos válidos")

    # --- 2. Procesar JSONs (que ya están en memoria) ---
    if not intrinsics_data:
        raise Failure("No se encontró intrinsics.json dentro del ZIP")
    if not results_data:
        raise Failure("No se encontró results.json dentro del ZIP")

    try:
        intr_json = json.loads(intrinsics_data.decode("utf-8"))
        intr_raw = intr_json.get("raw")
        if not isinstance(intr_raw, str):
            raise Failure("intrinsics.json no contiene el campo 'raw' (string)")
            
        res_json = json.loads(results_data.decode("utf-8"))
        thickness = res_json.get("thickness_mm")
    except Exception as e:
        raise Failure(f"Error al parsear JSONs desde el ZIP: {e}")

    # --- 3. Construir datos.csv ---
    record_id = os.path.splitext(os.path.basename(key))[0]
    df = pd.DataFrame([{
        "record_id": record_id,
        "timestamp": ts_local.isoformat(),
        "intrinsics": intr_raw,
        "thickness": thickness,
    }])
    
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    out_key = f"{prefix.rstrip('/')}/datos.csv"
    
    # Añadir el CSV a la lista de subida
    files_to_upload.append((out_key, csv_bytes, "text/csv; charset=utf-8"))
    
    # --- 4. Subir todos los archivos a S3 ---
    for dest_key, data, ctype in files_to_upload:
        try:
            s3.put_object(Bucket=BUCKET, Key=dest_key, Body=data, ContentType=ctype)
            created.append(dest_key)
        except Exception as e:
            logger.warning(f"Error subiendo {dest_key}: {e}")

    logger.info(f"Total subidos: {len(created)} archivos → prefijo s3://{BUCKET}/{prefix}/")
    
    # Devolver la clave del CSV, que es la dependencia para el siguiente paso
    return out_key


@op(
    ins={
        "processed_csv_key": In(str, description="Clave del datos.csv en bronze. Usado para asegurar dependencia."),
        "source_key": In(default_value=None, description="Clave S3 original (del tag del run)")
    }
)
def move_original_file_to_archive(  # mantiene el nombre por compatibilidad
    context: OpExecutionContext,
    processed_csv_key: str,
    source_key: str | None
):
    """
    Tras crear datos.csv, elimina el ZIP original.
    No copia ni mueve a ninguna carpeta de 'uploads/'. No deja rastro.
    """
    logger = get_dagster_logger()

    key = source_key or context.run.tags.get("s3_key")
    if not key:
        raise Failure("No se encontró 's3_key' en los tags del run para borrar el ZIP original.")

    if not processed_csv_key:
        # Si por algún motivo no se generó el CSV, no borres el original.
        logger.warning(f"No se generó CSV para {key}; se mantiene el ZIP original.")
        return None

    s3 = make_s3()
    try:
        s3.delete_object(Bucket=BUCKET, Key=key)
        logger.info(f"Eliminado ZIP original: s3://{BUCKET}/{key}")
        return None
    except Exception as e:
        logger.error(f"Fallo al eliminar {key}: {e}")
        raise Failure(f"Fallo al eliminar {key}: {e}")


# ---------- Job ----------
@job
def ingest_job():
    payload = download_from_s3()                # key llega por tags
    _ext    = validate_file(payload)            # key llega por tags
    
    # Esta 'op' ahora hace todo: descomprime, procesa JSONs, crea CSV y sube todo
    _out_csv = process_zip_and_upload(payload)  # key llega por tags
    
    move_original_file_to_archive(
        processed_csv_key=_out_csv              # source_key llega por tags
    )


# ---------- Sensor ----------
@sensor(
    job=ingest_job,
    minimum_interval_seconds=60 
)
def s3_process_existing_files_sensor(context):
    """
    Sensor "sin estado" (stateless) que procesa CUALQUIER .zip que encuentre.
    Añadimos logs para depurar y un LÍMITE por tick.
    """
    # LÍMITE: Solo procesa 500 archivos por tick para evitar sobrecargar la DB
    RUN_REQUEST_LIMIT_PER_TICK = 500 
    
    logger = get_dagster_logger() 
    clean_ingest_prefix = (INGEST_PREFIX or "").rstrip('/') + '/'

    if not BUCKET or not INGEST_PREFIX:
        logger.error(f"[Sensor] S3_BUCKET o INGEST_PREFIX no están configurados. El sensor no puede funcionar.")
        return

    logger.info(f"[Sensor] Verificando S3 en: s3://{BUCKET}/{clean_ingest_prefix}")

    try:
        run_requests_generated = 0
        
        for obj in _list_objects(clean_ingest_prefix):
            # Si ya hemos alcanzado el límite, paramos
            if run_requests_generated >= RUN_REQUEST_LIMIT_PER_TICK:
                logger.info(f"[Sensor] Límite de {RUN_REQUEST_LIMIT_PER_TICK} RunRequests alcanzado. Se procesarán más en el siguiente tick.")
                break # <-- Salir del bucle for

            key = obj["Key"]
            logger.debug(f"[Sensor] Encontrado: {key}")

            # 1. Filtro de .zip
            if not key.lower().endswith(".zip"):
                continue
                
            # 2. Filtro de raíz
            relative_path = key[len(clean_ingest_prefix):]
            if '/' in relative_path:
                continue

            lm_iso = obj["LastModified"].astimezone(timezone.utc).isoformat()
            
            logger.info(f"[Sensor] ✅ Lanzando job para: {key}")
            
            yield RunRequest(
                run_key=f"ingest_{key}",
                tags={"s3_key": key, "s3_last_modified": lm_iso},
            )
            run_requests_generated += 1

    except Exception as e:
        logger.error(f"[Sensor] Error al listar objetos S3: {e}", exc_info=True)