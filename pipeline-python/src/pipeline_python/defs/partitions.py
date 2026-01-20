"""
Partition definitions for Jackfruit assets.

Both ingestion and transformation use daily partitions aligned with data dates.
"""
import dagster as dg

# Daily partitions starting from when we began collecting data
# Adjust start_date to your actual data start
daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-01-01",
    timezone="UTC",
    end_offset=1,
)
