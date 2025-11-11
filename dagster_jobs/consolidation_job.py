import os
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Iterable, List, Tuple
import boto3
import pandas as pd
from botocore import exceptions as boto_exceptions
from botocore.config import Config
from dagster import (
    job, op, In, Out, OpExecutionContext, Failure, get_dagster_logger,
    ScheduleDefinition
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

BUCKET = os.getenv("S3_BUCKET")
ICEBERG_CATALOG   = os.getenv("ICEBERG_CATALOG")
ICEBERG_DB        = os.getenv("ICEBERG_DB")
HMS_URI           = os.getenv("HIVE_METASTORE_URI", "thrift://metastore:9083")
S3_PATH_STYLE     = os.getenv("S3_PATH_STYLE", "true")
S3_SSL_ENABLED    = os.getenv("S3_SSL_ENABLED", "true")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "data")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver")

#  -------------------- Configuración --------------------
try:
    # Reusar utilidades si existen en ingest_job
    from .ingest_job import TZ_EUROPE_MAD, make_s3 
except Exception:
    from zoneinfo import ZoneInfo
    TZ_EUROPE_MAD = ZoneInfo("Europe/Madrid")

    _S3_CLIENT_CACHE: dict[str, "boto3.client"] = {}

    def _bucket_region(bucket: str) -> str:
        probe = boto3.client("s3")
        try:
            resp = probe.get_bucket_location(Bucket=bucket)
        except boto_exceptions.ClientError as e:
            raise Failure(f"No se pudo resolver la región del bucket '{bucket}': {e}")
        return resp.get("LocationConstraint") or "eu-west-1"

    def make_s3() -> "boto3.client":
        bucket = os.getenv("S3_BUCKET")
        if not bucket:
            raise Failure("S3_BUCKET env var is required")
        if bucket in _S3_CLIENT_CACHE:
            return _S3_CLIENT_CACHE[bucket]
        region = _bucket_region(bucket)
        client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            config=Config(signature_version="s3v4"),
        )
        _S3_CLIENT_CACHE[bucket] = client
        return client

def _list_objects(prefix: str) -> Iterable[dict]:
    s3 = make_s3()
    token = None
    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) if "Contents" in resp else []:
            if not obj["Key"].endswith("/"):
                yield obj
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

def bronze_day_prefix(y: int, m: int, d: int) -> str:
    # prefijo bronze por día (donde están los datos.csv diarios)
    return f"{BRONZE_PREFIX.rstrip('/')}/{y:04d}/{m:02d}/{d:02d}/"

def _silver_day_prefix(y: int, m: int, d: int) -> str:
    # Particionado tipo Hive
    return f"{SILVER_PREFIX.rstrip('/')}/year={y:04d}/month={m:02d}/day={d:02d}/"

#  -------------------- Ops --------------------
@op(out=Out(Tuple[int,int,int], description="(year, month, day)"))
def resolve_target_day(context: OpExecutionContext) -> Tuple[int,int,int]:
    """Día de consolidación -> hoy (Europe/Madrid). No necesita run_config."""
    now_local = datetime.now(timezone.utc).astimezone(TZ_EUROPE_MAD)
    yesterday_local = now_local - timedelta(days=1)
    y, m, d = yesterday_local.year, yesterday_local.month, yesterday_local.day
    get_dagster_logger().info(f"Consolidando para el día: {y:04d}-{m:02d}-{d:02d}")
    return (y, m, d)

@op(ins={"ymd": In(Tuple[int,int,int])},
    out=Out(List[str], description="Keys de los datos.csv a consolidar"))
def list_day_csvs(context: OpExecutionContext, ymd: Tuple[int,int,int]) -> List[str]:
    y, m, d = ymd
    prefix = bronze_day_prefix(y, m, d)
    keys: List[str] = []
    for obj in _list_objects(prefix):
        key = obj["Key"]
        if key.lower().endswith("/datos.csv"):
            keys.append(key)
    if not keys:
        raise Failure(f"No se encontraron datos.csv en s3://{BUCKET}/{prefix}")
    get_dagster_logger().info(f"Encontrados {len(keys)} CSV para {y:04d}-{m:02d}-{d:02d}")
    return keys

@op(ins={"csv_keys": In(List[str])}, out=Out(pd.DataFrame))
def download_and_concat(csv_keys: List[str]) -> pd.DataFrame:
    s3 = make_s3()
    frames: List[pd.DataFrame] = []
    col_types = {"record_id": str}
    for k in csv_keys:
        obj = s3.get_object(Bucket=BUCKET, Key=k)
        df = pd.read_csv(obj["Body"], dtype=col_types)
        # Normalizar columnas esperadas
        for col in ("record_id", "timestamp", "intrinsics", "thickness"):
            if col not in df.columns:
                df[col] = pd.NA
        frames.append(df[["record_id", "timestamp", "intrinsics", "thickness"]])
    if not frames:
        raise Failure("No se pudieron leer CSVs para consolidar.")
    out = pd.concat(frames, ignore_index=True)
    return out

@op(ins={"df": In(pd.DataFrame), "ymd": In(Tuple[int,int,int])},
    out=Out(str, description="S3 key del CSV consolidado"))
def upload_consolidated_csv(df: pd.DataFrame, ymd: Tuple[int,int,int]) -> str:
    y, m, d = ymd
    out_key = f"{bronze_day_prefix(y, m, d)}_consolidated/datos_consolidated.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    s3 = make_s3()
    s3.put_object(
        Bucket=BUCKET,
        Key=out_key,
        Body=csv_bytes,
        ContentType="text/csv; charset=utf-8",
    )
    get_dagster_logger().info(f"Subido CSV consolidado a s3://{BUCKET}/{out_key}")
    return out_key

@op(ins={"df": In(pd.DataFrame), "ymd": In(Tuple[int,int,int])},
    out=Out(str, description="S3 key del Parquet consolidado en Silver (particionado por día)"))
def upload_consolidated_parquet(df: pd.DataFrame, ymd: Tuple[int,int,int]) -> str:
    y, m, d = ymd
    # Ruta en Silver particionada por día
    out_key = f"{_silver_day_prefix(y, m, d)}datos_consolidated.parquet"

    # Escritura Parquet
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO

    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    # Subir a S3
    s3 = make_s3()
    s3.put_object(
        Bucket=BUCKET,
        Key=out_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream"
    )

    get_dagster_logger().info(f"Subido Parquet consolidado (Silver) a s3://{BUCKET}/{out_key}")
    return out_key

@op(
    ins={
        "parquet_key": In(str, description="S3 key del Parquet consolidado"),
        "ymd": In(Tuple[int,int,int]),
    },
    out=Out(str, description="Tabla Iceberg a la que se ha hecho INSERT"),
)
def register_consolidated_in_iceberg(context: OpExecutionContext, parquet_key: str, ymd: Tuple[int,int,int]) -> str:
    """
    Registra el Parquet consolidado en una tabla Iceberg particionada por (year, month, day).
    Tabla destino: <ICEBERG_CATALOG>.<ICEBERG_DB>.datos_consolidated
    """
    logger = get_dagster_logger()
    if not parquet_key:
        raise Failure("parquet_key vacío; nada que registrar en Iceberg")

    y, m, d = ymd
    full_table = f"{ICEBERG_CATALOG}.{ICEBERG_DB}.datos_consolidated"
    parquet_uri = f"s3a://{BUCKET}/{parquet_key}"

    spark = (
        SparkSession.builder.appName("register-consolidated-iceberg")
        # Paquetes
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        # Iceberg + Hive
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hive")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.uri", HMS_URI)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", f"s3a://{BUCKET}/silver")
        # Metastore Hive
        .config("spark.hadoop.hive.metastore.uris", HMS_URI)
        # S3A
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", S3_SSL_ENABLED)
        .config("spark.hadoop.fs.s3a.path.style.access", S3_PATH_STYLE)
    )

    if os.getenv("AWS_REGION"):
        spark = spark.config("spark.hadoop.fs.s3a.region", os.getenv("AWS_REGION"))

    spark = spark.enableHiveSupport().getOrCreate()

    try:
        logger.info(f"Leyendo Parquet consolidado: {parquet_uri}")
        df = spark.read.parquet(parquet_uri)

        # Añadimos particiones desde ymd
        df2 = df.withColumn("year", lit(int(y))).withColumn("month", lit(int(m))).withColumn("day", lit(int(d)))
        df2.createOrReplaceTempView("to_append")

        # Asegura DB
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}")

        # Crea tabla vacía si no existe, con la partición correcta
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {full_table}
                USING iceberg
                PARTITIONED BY (year, month, day)
                AS SELECT * FROM to_append WHERE 1=0"""
        )

        # Inserta los datos del día
        inserted = spark.sql(f"INSERT INTO {full_table} SELECT * FROM to_append")
        logger.info(f"INSERT completado en {full_table}. Rows: {inserted.rowcount if hasattr(inserted,'rowcount') else 'n/a'}")

        # Opcional: recuento tras insertar
        total = spark.table(full_table).count()
        logger.info(f"Filas totales en {full_table}: {total}")

        return full_table
    except Exception as e:
        logger.error(f"Error registrando en Iceberg: {e}")
        raise Failure(f"Fallo registrando Parquet en Iceberg: {e}")
    finally:
        spark.stop()

# -------------------- Job --------------------
@job
def consolidation_job():
    ymd = resolve_target_day()
    keys = list_day_csvs(ymd)
    df   = download_and_concat(keys)
    _csv = upload_consolidated_csv(df, ymd)
    pq   = upload_consolidated_parquet(df, ymd)
    _tbl = register_consolidated_in_iceberg(parquet_key=pq, ymd=ymd)

#  Schedule diario a las 23:59 (Europe/Madrid)
daily_consolidation_schedule = ScheduleDefinition(
    name="daily_consolidation_schedule",
    cron_schedule="59 23 * * *",
    job=consolidation_job,
    execution_timezone="Europe/Madrid",
    run_config={},
    tags={"purpose": "daily-consolidation"},
)
