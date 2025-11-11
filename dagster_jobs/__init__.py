from dagster import Definitions
from .ingest_job import ingest_job, s3_process_existing_files_sensor
from .consolidation_job import consolidation_job, daily_consolidation_schedule

defs = Definitions(
    jobs=[ingest_job, consolidation_job],
    sensors=[s3_process_existing_files_sensor],   # (si ya lo tienes para ingest)
    schedules=[daily_consolidation_schedule],
)
