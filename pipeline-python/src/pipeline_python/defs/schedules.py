"""
Schedules for automated Jackfruit pipeline execution.

Daily schedules trigger materialization of partitioned assets.
Dagster handles partitioning, so we request specific partition keys and let
Dagster execute asset dependencies in order.

Example: CAMS daily schedule at 08:00 UTC materializes today's data
(because CAMS data is typically available ~6 hours after midnight UTC).
"""
import dagster as dg

from .partitions import daily_partitions


@dg.schedule(
    job=dg.define_asset_job(
        "cams_daily_job",
        partitions_def=daily_partitions,
        tags={"pipeline": "cams"},
        selection=["ingest_cams_data", "transform_cams_data"],
    ),
    cron_schedule="0 8 * * *",  # 08:00 UTC every day
    execution_timezone="UTC",
)
def cams_daily_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """
    Daily schedule to materialize CAMS ingestion and transformation.

    Runs at 08:00 UTC to process today's data. CAMS forecast data is
    typically available ~6 hours after midnight UTC, so an 8am run provides
    sufficient buffer.

    The job depends on both ingest_cams_data and transform_cams_data assets.
    Dagster executes them in dependency order (ingestion first, then transformation).

    Args:
        context: Dagster schedule evaluation context

    Returns:
        RunRequest for today's partition, with tags for observability
    """
    # Calculate today's date (the data we want to process)
    scheduled_date = context.scheduled_execution_time.date()
    partition_key = scheduled_date.strftime("%Y-%m-%d")

    return dg.RunRequest(
        run_key=f"cams_daily_{partition_key}",
        partition_key=partition_key,
        tags={
            "source": "schedule",
            "pipeline": "cams",
            "scheduled_date": partition_key,
        },
    )


@dg.schedule(
    job=dg.define_asset_job(
        "ecmwf_daily_job",
        partitions_def=daily_partitions,
        tags={"pipeline": "ecmwf"},
        selection=["ingest_ecmwf_data"]
    ),
    cron_schedule="30 9 * * *",
    execution_timezone="UTC",
)
def ecmwf_daily_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """
    Daily schedule to materialize ECMWF weather forecast ingestion.

    Runs at 09:30 UTC to process today's data. ECMWF IFS 00Z forecast data is
    typically available ~09:00 UTC, so a 09:30 run provides sufficient buffer.

    Args:
        context: Dagster schedule evaluation context

    Returns:
        RunRequest for today's partition, with tags for observability
    """
    scheduled_date = context.scheduled_execution_time.date()
    partition_key = scheduled_date.strftime("%Y-%m-%d")
    return dg.RunRequest(
        run_key=f"ecmwf_daily_{partition_key}",
        partition_key=partition_key,
        tags={
            "source": "schedule",
            "pipeline": "ecmwf",
            "scheduled_date": partition_key,
        },
    )
