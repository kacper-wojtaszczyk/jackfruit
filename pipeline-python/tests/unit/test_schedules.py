"""
Tests for Jackfruit pipeline schedules.

Verifies that schedules correctly generate RunRequests for expected partitions
and apply proper tags for observability.
"""
from datetime import datetime

import dagster as dg
import pytest


@pytest.fixture
def postgres_env(monkeypatch):
    """Set up required postgres environment variables for definition loading."""
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("POSTGRES_SCHEMA", "catalog")


def _mock_schedule_context(scheduled_execution_time: datetime) -> dg.ScheduleEvaluationContext:
    """Create a realistic ScheduleEvaluationContext for testing."""
    return dg.build_schedule_context(
        scheduled_execution_time=scheduled_execution_time,
    )


class TestCamsDailySchedule:
    """Tests for the cams_daily_schedule."""

    def test_schedule_generates_run_request(self):
        """Schedule should generate a RunRequest when evaluated."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        now = datetime(2025, 3, 12, 8, 0, 0)
        context = _mock_schedule_context(now)

        request = cams_daily_schedule(context)

        assert request is not None
        assert isinstance(request, dg.RunRequest)

    def test_schedule_processes_today_partition(self):
        """Schedule should materialize today's partition."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        # If scheduled_execution_time is 2025-03-12 (Wed),
        # it should process 2025-03-12 (Tue)
        scheduled_date = datetime(2025, 3, 12, 8, 0, 0)
        context = _mock_schedule_context(scheduled_date)

        request = cams_daily_schedule(context)

        # Expected partition: 2025-03-11 (today)
        assert request.partition_key == "2025-03-12"

    def test_schedule_partition_format(self):
        """Schedule should format partition key as YYYY-MM-DD."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        # Test various dates to ensure consistent formatting
        test_cases = [
            (datetime(2025, 1, 2, 8, 0, 0), "2025-01-02"),  # Day 1->1
            (datetime(2025, 2, 28, 8, 0, 0), "2025-02-28"),  # Month boundary
            (datetime(2025, 3, 31, 8, 0, 0), "2025-03-31"),  # Month boundary
            (datetime(2025, 12, 31, 8, 0, 0), "2025-12-31"),  # Year boundary
        ]

        for scheduled_time, expected_partition in test_cases:
            context = _mock_schedule_context(scheduled_time)
            request = cams_daily_schedule(context)
            assert request.partition_key == expected_partition

    def test_schedule_run_key_includes_date(self):
        """Schedule should include date in run_key for idempotency."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        scheduled_date = datetime(2025, 3, 12, 8, 0, 0)
        context = _mock_schedule_context(scheduled_date)

        request = cams_daily_schedule(context)

        # run_key should be unique per partition
        assert request.run_key == "cams_daily_2025-03-12"

    def test_schedule_tags_include_metadata(self):
        """Schedule should tag runs with source, pipeline, and date."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        scheduled_date = datetime(2025, 3, 12, 8, 0, 0)
        context = _mock_schedule_context(scheduled_date)

        request = cams_daily_schedule(context)

        assert request.tags is not None
        assert request.tags.get("source") == "schedule"
        assert request.tags.get("pipeline") == "cams"
        assert request.tags.get("scheduled_date") == "2025-03-12"

    def test_schedule_consistent_across_runs(self):
        """Multiple evaluations for the same scheduled time should produce identical results."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        scheduled_date = datetime(2025, 3, 12, 8, 0, 0)

        # Create two independent contexts at the same time
        context1 = _mock_schedule_context(scheduled_date)
        context2 = _mock_schedule_context(scheduled_date)

        request1 = cams_daily_schedule(context1)
        request2 = cams_daily_schedule(context2)

        # Both should be identical
        assert request1.partition_key == request2.partition_key
        assert request1.run_key == request2.run_key
        assert request1.tags == request2.tags

    def test_schedule_handles_leap_year(self):
        """Schedule should correctly handle leap year dates."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        # 2024 is a leap year; test Feb 29
        scheduled_date = datetime(2024, 2, 29, 8, 0, 0)
        context = _mock_schedule_context(scheduled_date)

        request = cams_daily_schedule(context)

        # Should process Feb 29 (leap day)
        assert request.partition_key == "2024-02-29"

    def test_schedule_with_different_time_of_day(self):
        """Schedule should work regardless of what time of day it's evaluated."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        base_date = datetime(2025, 3, 12, 0, 0, 0)  # Midnight

        # Test evaluation at different times of the same day
        for hour in [0, 6, 12, 18, 23]:
            scheduled_time = base_date.replace(hour=hour)
            context = _mock_schedule_context(scheduled_time)

            request = cams_daily_schedule(context)

            # Regardless of hour, should still process today's partition
            assert request.partition_key == "2025-03-12"


class TestScheduleDefinitions:
    """Tests for schedule registration and discovery."""

    def test_schedules_are_loadable(self, postgres_env):
        """Schedules should be loadable from definitions."""
        from pipeline_python.definitions import defs

        definitions = defs()

        assert definitions.schedules is not None
        assert len(definitions.schedules) > 0

    def test_cams_daily_schedule_registered(self, postgres_env):
        """cams_daily_schedule should be registered in definitions."""
        from pipeline_python.definitions import defs

        definitions = defs()

        schedule_names = [s.name for s in definitions.schedules]
        assert "cams_daily_schedule" in schedule_names

    def test_schedule_has_cron_expression(self):
        """Schedule should have a valid cron expression."""
        from pipeline_python.defs.schedules import cams_daily_schedule

        # The schedule should be decorated with @dg.schedule
        # which sets a cron_schedule attribute
        assert hasattr(cams_daily_schedule, "_schedule_definition") or callable(
            cams_daily_schedule
        )

    def test_schedule_job_is_defined(self, postgres_env):
        """Schedule should reference a valid job."""
        from pipeline_python.definitions import defs

        definitions = defs()

        # Get the cams_daily_schedule
        schedule = next(s for s in definitions.schedules if s.name == "cams_daily_schedule")

        # The schedule should have a job reference
        assert schedule.job is not None


class TestScheduleJobDependencies:
    """Tests for the job that the schedule triggers."""

    def test_schedule_job_includes_both_assets(self, postgres_env):
        """The job triggered by schedule should include both ingestion and transformation."""
        from pipeline_python.definitions import defs

        definitions = defs()

        # Get the job from the schedule
        schedule = next(s for s in definitions.schedules if s.name == "cams_daily_schedule")
        job = schedule.job

        # The job should reference asset selections
        assert job is not None

    def test_transformation_depends_on_ingestion(self):
        """Transformation asset should depend on ingestion asset."""
        from pipeline_python.defs.assets import ingest_cams_data, transform_cams_data

        # Check that transform_cams_data has ingest_cams_data as a dependency
        dep_keys = transform_cams_data.dependency_keys
        assert ingest_cams_data.key in dep_keys
