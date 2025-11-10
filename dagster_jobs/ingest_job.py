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


# ---------- Config ----------
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/work/dbt")
BUCKET        = os.getenv("S3_BUCKET")
INGEST_PREFIX = os.getenv("INGEST_PREFIX")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")
TZ_EUROPE_MAD = ZoneInfo("Europe/Madrid")
_S3_CLIENT_CACHE: dict[str, boto3.client] = {}

dbt = DbtCliResource(project_dir="/work/dbt", profiles_dir=DBT_PROFILES_DIR)


# ---------- Helpers ----------
def _bucket_region(bucket: str) -> str:
    """
    Descubre la región real del bucket usando un cliente neutro.
    En us-east-1, LocationConstraint suele venir como None.
    """
    probe = boto3.client("s3")
    resp = probe.get_bucket_location(Bucket=bucket)
    return resp.get("LocationConstraint") or "eu-west-1"

def make_s3():
    """
    Crea un cliente S3 de AWS en la región real del bucket.
    NO usa S3_ENDPOINT. Usa credenciales AWS_*.
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
                    # si es dict con una clave principal que contiene lista
                    # intenta normalizar; si no, crea DF de una fila
                    for v in obj.values():
                        if isinstance(v, list):
                            df = pd.json_normalize(v)
                            break
                    else:
                        df = pd.json_normalize(obj)
                else:
                    raise ValueError("JSON no tabular")
            except Exception:
                # Caso 2: NDJSON (una entidad por línea)
                lines = [ln for ln in text.splitlines() if ln.strip()]
                rows = []
                for ln in lines:
                    try:
                        rows.append(json.loads(ln))
                    except Exception:
                        # si alguna línea no es JSON, fallo total para no mezclar
                        raise Failure(f"JSON NDJSON no válido en {name}")
                df = pd.json_normalize(rows)
        df["source_sheet"] = pd.NA
    else:
        raise Failure(f"Unsupported inner file type for {name}")
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
    df["source_file"] = os.path.basename(name)
    return df


# ---------- Ops ----------
@op
def download_from_s3(key: str) -> bytes:
    s3 = make_s3()
    buf = BytesIO()
    s3.download_fileobj(Bucket=BUCKET, Key=key, Fileobj=buf)
    buf.seek(0)
    get_dagster_logger().info(f"Downloaded {key} ({buf.getbuffer().nbytes} bytes)")
    return buf.getvalue()

@op
def validate_file(payload: bytes, key: str) -> str:
    k = key.lower()
    if k.endswith(".zip"):  return "zip"
    raise Failure(f"The file {key} is not an Excel file (.xlsx/.xls/.csv/.zip)")

@op(
    out={
        "uploaded_keys": Out(List[str], description="Lista de S3 keys subidas"),
        "job_prefix": Out(str, description="El prefijo S3 único para este job (ej: .../{job_uid})")
    }
)
def uncompress_zip(context: OpExecutionContext, payload: bytes, key: str): # <-- Quita el return type hint
    """
    Descomprime `payload` (ZIP) y sube todos los archivos a:
      s3://<BUCKET>/data/<YYYY>/<MM>/<DD>/<job_uid>/<nombre_del_fichero>
    Devuelve la lista de claves Y el prefijo del job.
    """
    logger = get_dagster_logger()

    if not key.lower().endswith(".zip"):
        logger.info(f"uncompress_zip: {key} no es .zip, no se hace nada.")
        # Devuelve valores vacíos para ambas salidas
        yield Output([], "uploaded_keys")
        yield Output("", "job_prefix") # O puedes fallar, pero esto es más seguro
        return

    try:
        zf = ZipFile(BytesIO(payload))
    except BadZipFile:
        raise Failure("ZIP corrupto o no válido")

    s3 = make_s3()
    created: list[str] = []

    # --- Prefijo fecha ---
    lm_iso = context.run.tags.get("s3_last_modified")
    ts_utc = datetime.fromisoformat(lm_iso) if lm_iso else datetime.now(timezone.utc)
    ts_local = ts_utc.astimezone(TZ_EUROPE_MAD)

    # Usa el código del folder del ZIP + UID(6)
    zip_stem = os.path.splitext(os.path.basename(key))[0]  # p.ej., 'mi_paquete' de '.../mi_paquete.zip'
    uid6 = uuid.uuid4().hex[:6]
    job_uid = f"{zip_stem}_{uid6}"

    # El prefijo ahora incluye el nuevo subnivel único
    prefix = f"data/{ts_local.year}/{ts_local.month:02d}/{ts_local.day:02d}/{job_uid}"

    # --- Recorremos los archivos del ZIP ---
    for zi in zf.infolist():
        name = zi.filename
        if name.endswith("/") or "__MACOSX" in name or os.path.basename(name).startswith("~$"):
            continue
        with zf.open(zi, "r") as fp:
            data = fp.read()
        filename = os.path.basename(name)
        dest_key = f"{prefix}/{filename}"
        ctype, _ = mimetypes.guess_type(filename)
        if not ctype:
            ctype = "application/octet-stream"
        try:
            s3.put_object(Bucket=BUCKET, Key=dest_key, Body=data, ContentType=ctype)
            created.append(dest_key)
            logger.info(f"Subido: s3://{BUCKET}/{dest_key}")
        except Exception as e:
            logger.warning(f"Error subiendo {filename}: {e}")

    if not created:
        raise Failure("El ZIP no contenía archivos válidos para subir")

    logger.info(f"Total subidos: {len(created)} archivos → prefijo s3://{BUCKET}/{prefix}/")
    
    yield Output(created, "uploaded_keys")
    yield Output(prefix, "job_prefix")

@op
def build_datos_csv_from_unzip(
    context: OpExecutionContext, 
    uploaded_keys: List[str], 
    key: str, 
    job_prefix: str  # <-- NUEVA ENTRADA
) -> str:
    """
    Crea un CSV 'datos.csv' DENTRO de la carpeta única del job_prefix.
    s3://<BUCKET>/data/<YYYY>/<MM>/<DD>/<job_uid>/datos.csv
    """
    logger = get_dagster_logger()
    s3 = make_s3()

    # Si no se subieron archivos (ej. ZIP vacío), no hacer nada
    if not uploaded_keys or not job_prefix:
        logger.info("No se subieron archivos, saltando creación de datos.csv")
        return ""

    # Fecha local (para el timestamp)
    lm_iso = context.run.tags.get("s3_last_modified")
    ts_utc = datetime.fromisoformat(lm_iso) if lm_iso else datetime.now(timezone.utc)
    ts_local = ts_utc.astimezone(TZ_EUROPE_MAD)
    
    # record_id desde el nombre del ZIP
    record_id = os.path.splitext(os.path.basename(key))[0]

    # localiza las rutas subidas de intrinsics y results
    intrinsics_key = None
    results_key = None
    for k in uploaded_keys:
        base = os.path.basename(k).lower()
        if base == "intrinsics.json":
            intrinsics_key = k
        elif base == "results.json":
            results_key = k

    if not intrinsics_key:
        raise Failure("No se encontró intrinsics.json dentro del ZIP")
    if not results_key:
        raise Failure("No se encontró results.json dentro del ZIP")

    # descarga y parsea intrinsics.json
    intr_obj = s3.get_object(Bucket=BUCKET, Key=intrinsics_key)
    intr_json = json.loads(intr_obj["Body"].read().decode("utf-8"))
    intr_raw = intr_json.get("raw")
    if not isinstance(intr_raw, str):
        raise Failure("intrinsics.json no contiene el campo 'raw' (string)")

    # descarga y parsea results.json
    res_obj = s3.get_object(Bucket=BUCKET, Key=results_key)
    res_json = json.loads(res_obj["Body"].read().decode("utf-8"))
    thickness = res_json.get("thickness_mm")

    # construye DataFrame con una fila
    df = pd.DataFrame([{
        "record_id": record_id,
        "timestamp": ts_local.isoformat(),
        "intrinsics": intr_raw,
        "thickness": thickness,
    }])

    # serializa a CSV (sin índice)
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # Usa el job_prefix y le añade /datos.csv
    out_key = f"{job_prefix.rstrip('/')}/datos.csv"
    
    s3.put_object(
        Bucket=BUCKET,
        Key=out_key,
        Body=csv_bytes,
        ContentType="text/csv; charset=utf-8"
    )
    logger.info(f"Creado CSV: s3://{BUCKET}/{out_key}")
    return out_key


@op(ins={"key": In(default_value=None, description="Source S3 key (optional, taken from tags if not provided)")})
def upload_dataframe_as_parquet(context: OpExecutionContext, df: pd.DataFrame, key: str | None) -> str:
    key = key or context.run.tags.get("s3_key")
    lm_iso = context.run.tags.get("s3_last_modified")
    ts_utc = datetime.fromisoformat(lm_iso) if lm_iso else datetime.now(timezone.utc)
    parts = _local_parts(ts_utc)

    df = df.copy()
    df["year"], df["month"], df["day"] = (
        parts["y"], int(parts["m"]), int(parts["d"])
    )

    base = os.path.basename(key)
    stem = os.path.splitext(base)[0]
    uid = f"{int(datetime.now(timezone.utc).timestamp())%100000:05d}"
    out_key = (
        f"{SILVER_PREFIX.rstrip('/')}/"
        f"year={parts['y']}/month={parts['m']}/day={parts['d']}/"
        f"{stem}_{uid}.parquet"
    )

    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3 = make_s3()
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
    get_dagster_logger().info(
        f"Uploaded: s3://{BUCKET}/{out_key} (partition {parts['y']}-{parts['m']}-{parts['d']} Europe/Madrid)"
    )
    return out_key

# ---------- Job ----------
@job
def ingest_job():
    payload = download_from_s3()                     
    _ext    = validate_file(payload)                 
    unzip_results = uncompress_zip(payload)
    _out_csv = build_datos_csv_from_unzip(
        uploaded_keys=unzip_results.uploaded_keys,
        job_prefix=unzip_results.job_prefix
    )

# ---------- Sensor ----------
@sensor(job=ingest_job, minimum_interval_seconds=3)
def s3_new_objects_sensor(context):
    last_seen = context.cursor
    now_iso   = datetime.now(timezone.utc).isoformat()

    items = []
    for obj in _list_objects(INGEST_PREFIX):
        lm_iso = obj["LastModified"].astimezone(timezone.utc).isoformat()
        if last_seen is None or lm_iso > last_seen:
            items.append((obj["Key"], lm_iso))

    if not items:
        if last_seen is None:
            context.update_cursor(now_iso)
        return

    items.sort(key=lambda x: x[1])
    for key, lm_iso in items:
        yield RunRequest(
            run_key = f"{lm_iso}|{key}",
            run_config={
                "ops": {
                    "download_from_s3": {"inputs": {"key": {"value": key}}},
                    "validate_file":   {"inputs": {"key": {"value": key}}},
                    "uncompress_zip":   {"inputs": {"key": {"value": key}}},
                    "build_datos_csv_from_unzip": {"inputs": {"key": {"value": key}}},
                }
            },
            tags={"s3_key": key, "s3_last_modified": lm_iso},
        )

    context.update_cursor(items[-1][1])