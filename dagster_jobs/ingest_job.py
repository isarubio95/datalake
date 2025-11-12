import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Iterable
import boto3
import botocore
from botocore.config import Config
from dagster import (
    sensor, RunRequest, job, op, get_dagster_logger, OpExecutionContext, Failure,
)

# -------------------- Config --------------------
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/work/dbt")
BUCKET          = os.getenv("S3_BUCKET")
INGEST_PREFIX   = os.getenv("INGEST_PREFIX")
BRONZE_PREFIX   = os.getenv("BRONZE_PREFIX")  # opcional; no imprescindible
TZ_EUROPE_MAD   = ZoneInfo("Europe/Madrid")
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
            if code in {"PermanentRedirect", "AuthorizationHeaderMalformed", "IllegalLocationConstraintException", "301"}:
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


# -------------------- Op única: mover carpeta --------------------
@op
def move_source_folder_to_processed(context: OpExecutionContext) -> None:
    """
    Copia TODO el contenido de la carpeta origen (detectada por el sensor)
    a un prefijo de destino fechado (data/YYYY/MM/DD/<folder>_<uid>/) y
    elimina la carpeta original.
    """
    logger = get_dagster_logger()
    s3 = make_s3()

    source_prefix = context.run.tags.get("s3_prefix")
    if not source_prefix:
        raise Failure("Falta 's3_prefix' en los tags del run.")

    # Fecha de referencia (LastModified del intrinsics.json que disparó el sensor)
    lm_iso = context.run.tags.get("s3_last_modified")
    ts_utc = datetime.fromisoformat(lm_iso) if lm_iso else datetime.now(timezone.utc)
    ts_local = ts_utc.astimezone(TZ_EUROPE_MAD)

    folder_name = source_prefix.rstrip("/").split("/")[-1]
    # uid corto para evitar colisiones si re-procesas misma carpeta
    import uuid as _uuid
    uid6 = _uuid.uuid4().hex[:6]
    job_uid = f"{folder_name}_{uid6}"

    base = (BRONZE_PREFIX or "data").rstrip("/")

    dest_prefix = f"{base}/{ts_local.year}/{ts_local.month:02d}/{ts_local.day:02d}/{job_uid}"

    try:
        objects_to_move = list(_list_objects(source_prefix))
        if not objects_to_move:
            logger.warning(f"No se encontraron objetos para mover en {source_prefix}")
            return

        logger.info(f"Moviendo {len(objects_to_move)} objetos de {source_prefix} a {dest_prefix}...")

        to_delete = []

        # 1) Copiar todos los objetos
        for obj in objects_to_move:
            old_key = obj["Key"]
            relative = old_key[len(source_prefix):]
            new_key = f"{dest_prefix.rstrip('/')}/{relative}"

            s3.copy_object(CopySource={'Bucket': BUCKET, 'Key': old_key}, Bucket=BUCKET, Key=new_key)
            to_delete.append({'Key': old_key})

        logger.info(f"Copiados {len(to_delete)} objetos.")

        # 2) Borrar originales en lotes de 1000
        for i in range(0, len(to_delete), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={'Objects': to_delete[i:i+1000]})

        logger.info(f"Eliminada carpeta original: s3://{BUCKET}/{source_prefix}")

    except Exception as e:
        logger.error(f"Fallo moviendo/eliminando {source_prefix}: {e}")
        raise Failure(f"Fallo al mover/eliminar la carpeta: {e}")


# -------------------- Job --------------------
@job
def ingest_job():
    move_source_folder_to_processed()


# -------------------- Sensor --------------------
@sensor(job=ingest_job, minimum_interval_seconds=3)
def s3_process_existing_files_sensor(context):
    """
    Sensor sin estado: recorre INGEST_PREFIX y lanza un run por cada carpeta
    que contenga 'intrinsics.json'.
    """
    RUN_REQUEST_LIMIT_PER_TICK = 500
    logger = get_dagster_logger()
    clean_ingest_prefix = (INGEST_PREFIX or "").rstrip("/") + "/"

    if not BUCKET or not INGEST_PREFIX:
        logger.error("[Sensor] S3_BUCKET o INGEST_PREFIX no están configurados.")
        return

    logger.info(f"[Sensor] Escaneando s3://{BUCKET}/{clean_ingest_prefix}")

    try:
        generated = 0
        for obj in _list_objects(clean_ingest_prefix):
            if generated >= RUN_REQUEST_LIMIT_PER_TICK:
                logger.info(f"[Sensor] Límite de {RUN_REQUEST_LIMIT_PER_TICK} RunRequests alcanzado.")
                break

            key = obj["Key"]
            if os.path.basename(key).lower() == "intrinsics.json":
                source_prefix = os.path.dirname(key) + "/"
                if source_prefix == clean_ingest_prefix:
                    continue

                lm_iso = obj["LastModified"].astimezone(timezone.utc).isoformat()
                logger.info(f"[Sensor] Lanzando job para: {source_prefix}")

                yield RunRequest(
                    run_key=f"ingest_{source_prefix}",
                    tags={
                        "s3_prefix": source_prefix,
                        "s3_last_modified": lm_iso,
                    },
                )
                generated += 1

    except Exception as e:
        logger.error(f"[Sensor] Error listando S3: {e}", exc_info=True)
