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

# -------------------- Config --------------------
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/work/dbt")
BUCKET          = os.getenv("S3_BUCKET")
INGEST_PREFIX = os.getenv("INGEST_PREFIX")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")
TZ_EUROPE_MAD = ZoneInfo("Europe/Madrid")
_S3_CLIENT_CACHE: dict[str, boto3.client] = {}


# -------------------- Helpers --------------------
def _bucket_region(bucket: str) -> str:
    probe = boto3.client("s3")
    resp = probe.get_bucket_location(Bucket=bucket)
    return resp.get("LocationConstraint") or "eu-west-1"

def make_s3():
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
                "PermanentRedirect", "AuthorizationHeaderMalformed",
                "IllegalLocationConstraintException", "301",
            }:
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


# -------------------- Ops --------------------
@op(
    out={
        "csv_key": Out(str, description="La S3 key del 'datos.csv' generado."),
        "processed_prefix": Out(str, description="El prefijo S3 base donde se ha guardado el CSV.")
    }
)
def extract_csv_data_and_upload(context: OpExecutionContext):
    """
    Lee 'intrinsics.json' y 'results.json' directamente desde el prefijo S3,
    genera 'datos.csv' y sube *solo* ese CSV a S3.
    
    Devuelve la clave del CSV y el prefijo de destino para que la siguiente
    'op' mueva el resto de archivos allí.
    """
    logger = get_dagster_logger()
    s3 = make_s3()
    
    # El sensor ahora nos pasa el prefijo de la carpeta, ej: "uploads/job_123/"
    source_prefix = context.run.tags.get("s3_prefix")
    if not source_prefix:
        raise Failure("extract_csv_data_and_upload: falta 's3_prefix' en los tags.")

    intr_key = f"{source_prefix.rstrip('/')}/intrinsics.json"
    res_key = f"{source_prefix.rstrip('/')}/results.json"
    
    try:
        # --- 1. Leer los JSONs directamente de S3 ---
        logger.info(f"Leyendo {intr_key} y {res_key}...")
        intr_obj = s3.get_object(Bucket=BUCKET, Key=intr_key)
        intrinsics_data = intr_obj['Body'].read()
        
        res_obj = s3.get_object(Bucket=BUCKET, Key=res_key)
        results_data = res_obj['Body'].read()
        
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise Failure(f"No se encontró un archivo JSON requerido en {source_prefix}")
        raise Failure(f"Error de S3 al leer JSONs: {e}")

    # --- 2. Procesar JSONs (lógica idéntica a la anterior) ---
    try:
        intr_json = json.loads(intrinsics_data.decode("utf-8"))
        intr_raw = intr_json.get("raw")
        if not isinstance(intr_raw, str):
            raise Failure("intrinsics.json no contiene el campo 'raw' (string)")
            
        res_json = json.loads(results_data.decode("utf-8"))
        thickness = res_json.get("thickness_mm")
    except Exception as e:
        raise Failure(f"Error al parsear JSONs desde S3: {e}")

    # --- 3. Calcular prefijo de destino ---
    lm_iso = context.run.tags.get("s3_last_modified") # (Timestamp del intrinsics.json)
    ts_utc = datetime.fromisoformat(lm_iso) if lm_iso else datetime.now(timezone.utc)
    ts_local = ts_utc.astimezone(TZ_EUROPE_MAD)

    # Usamos el nombre de la carpeta de origen como base
    folder_name = source_prefix.rstrip('/').split('/')[-1]
    uid6 = uuid.uuid4().hex[:6]
    job_uid = f"{folder_name}_{uid6}"
    
    prefix = f"data/{ts_local.year}/{ts_local.month:02d}/{ts_local.day:02d}/{job_uid}"

    # --- 4. Construir y subir datos.csv ---
    df = pd.DataFrame([{
        "record_id": folder_name, # El ID es el nombre de la carpeta
        "timestamp": ts_local.isoformat(),
        "intrinsics": intr_raw,
        "thickness": thickness,
    }])
    
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    out_key = f"{prefix.rstrip('/')}/datos.csv"
    
    try:
        s3.put_object(
            Bucket=BUCKET, 
            Key=out_key, 
            Body=csv_bytes, 
            ContentType="text/csv; charset=utf-8"
        )
        logger.info(f"Subido CSV: s3://{BUCKET}/{out_key}")
    except Exception as e:
        logger.error(f"Error subiendo {out_key}: {e}")
        raise Failure(f"Error subiendo CSV: {e}")

    # Devolver la clave del CSV y el prefijo de destino
    return out_key, prefix


@op(
    ins={
        "csv_key": In(str, description="Clave del datos.csv. Usado para asegurar dependencia."),
        "processed_prefix": In(str, description="Prefijo de destino (ej: data/2025/10/01/job_123)"),
    }
)
def move_original_folder_to_processed(
    context: OpExecutionContext,
    csv_key: str,
    processed_prefix: str,
):
    """
    Mueve *todo* el contenido de la carpeta original (usando S3 CopyObject) 
    a la carpeta de procesados, junto al 'datos.csv' que ya se creó.
    Finalmente, elimina los archivos originales.
    """
    logger = get_dagster_logger()
    s3 = make_s3()
    
    source_prefix = context.run.tags.get("s3_prefix")
    if not source_prefix:
        raise Failure("No se encontró 's3_prefix' en los tags del run.")

    if not csv_key or not processed_prefix:
        logger.warning(f"No se generó CSV o prefijo para {source_prefix}; se mantiene la carpeta original.")
        return None

    try:
        objects_to_move = list(_list_objects(source_prefix))
        if not objects_to_move:
            logger.warning(f"No se encontraron objetos para mover en {source_prefix}")
            return None

        logger.info(f"Moviendo {len(objects_to_move)} objetos de {source_prefix} a {processed_prefix}...")

        objects_to_delete = []
        
        # --- 1. Copiar todos los objetos ---
        for obj in objects_to_move:
            old_key = obj["Key"]
            relative_key = old_key[len(source_prefix):]
            new_key = f"{processed_prefix.rstrip('/')}/{relative_key}"
            
            copy_source = {'Bucket': BUCKET, 'Key': old_key}
            s3.copy_object(CopySource=copy_source, Bucket=BUCKET, Key=new_key)
            
            # Añadir a la lista de borrado
            objects_to_delete.append({'Key': old_key})

        logger.info(f"Copiados {len(objects_to_delete)} objetos.")

        # --- 2. Eliminar los originales (en lote) ---
        # (delete_objects solo puede manejar 1000 claves a la vez)
        for i in range(0, len(objects_to_delete), 1000):
            chunk = objects_to_delete[i:i + 1000]
            s3.delete_objects(Bucket=BUCKET, Delete={'Objects': chunk})
            
        logger.info(f"Eliminada carpeta original: s3://{BUCKET}/{source_prefix}")
        
    except Exception as e:
        logger.error(f"Fallo al mover/eliminar la carpeta {source_prefix}: {e}")
        raise Failure(f"Fallo al mover/eliminar la carpeta: {e}")


# ---------- Job Modificado ----------
@job
def ingest_job():
    # Leer los JSONs desde S3 y subir el CSV
    csv_key, prefix = extract_csv_data_and_upload()
    
    # Esta 'op' mueve todos los archivos de la carpeta original (S3 Copy) y borra el original
    move_original_folder_to_processed(
        csv_key=csv_key,
        processed_prefix=prefix
    )


# ---------- Sensor Modificado ----------
@sensor(
    job=ingest_job,
    minimum_interval_seconds=3
)
def s3_process_existing_files_sensor(context):
    """
    Sensor "sin estado" que procesa CUALQUIER carpeta en el INGEST_PREFIX
    que contenga un archivo 'intrinsics.json'.
    """
    RUN_REQUEST_LIMIT_PER_TICK = 500 
    
    logger = get_dagster_logger() 
    clean_ingest_prefix = (INGEST_PREFIX or "").rstrip('/') + '/'

    if not BUCKET or not INGEST_PREFIX:
        logger.error(f"[Sensor] S3_BUCKET o INGEST_PREFIX no están configurados.")
        return

    logger.info(f"[Sensor] Verificando S3 en: s3://{BUCKET}/{clean_ingest_prefix}")

    try:
        run_requests_generated = 0
        
        # Usamos _list_objects para un listado profundo
        for obj in _list_objects(clean_ingest_prefix):
            if run_requests_generated >= RUN_REQUEST_LIMIT_PER_TICK:
                logger.info(f"[Sensor] Límite de {RUN_REQUEST_LIMIT_PER_TICK} RunRequests alcanzado.")
                break 

            key = obj["Key"]
            filename = os.path.basename(key).lower()

            # --- NUEVA LÓGICA DE DETECCIÓN ---
            # Buscamos 'intrinsics.json' como el archivo "trigger"
            if filename == "intrinsics.json":
                
                # Obtenemos el "directorio" que lo contiene
                source_prefix = os.path.dirname(key) + '/'
                
                # Ignoramos si está en la raíz del prefijo de ingesta
                if source_prefix == clean_ingest_prefix:
                    continue

                lm_iso = obj["LastModified"].astimezone(timezone.utc).isoformat()
                
                logger.info(f"[Sensor] ✅ Lanzando job para la carpeta: {source_prefix}")
                
                yield RunRequest(
                    # La run_key se basa en el prefijo de la carpeta
                    run_key=f"ingest_{source_prefix}", 
                    tags={
                        "s3_prefix": source_prefix, # Pasamos el prefijo de la carpeta
                        "s3_last_modified": lm_iso
                    },
                )
                run_requests_generated += 1

    except Exception as e:
        logger.error(f"[Sensor] Error al listar objetos S3: {e}", exc_info=True)