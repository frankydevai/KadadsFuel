from pathlib import Path


def test_periodic_alert_jobs_do_not_run_immediately_on_restart():
    """A deploy must wait for normal intervals instead of causing alert noise."""
    root = Path(__file__).resolve().parent.parent
    main = (root / "dieselup" / "main.py").read_text()

    assert "next_run_time=_now" not in main
    assert "IntervalTrigger(minutes=15)" in main
    assert main.count("IntervalTrigger(minutes=5)") == 3
    assert 'id="driver_assignments"' in main
