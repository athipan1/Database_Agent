from app.services.scheduler import RuntimeScheduler


def test_runtime_scheduler_has_no_partition_or_schema_job():
    scheduler = RuntimeScheduler()
    scheduler.configure(
        ingestion_job=lambda: None,
        stats_job=lambda: None,
    )

    jobs = list(scheduler._scheduler.jobs)
    assert len(jobs) == 2
    assert all("partition" not in repr(job).lower() for job in jobs)
    assert all("schema" not in repr(job).lower() for job in jobs)
