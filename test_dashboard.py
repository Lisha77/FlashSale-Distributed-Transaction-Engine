"""
Dashboard Smoke Test
--------------------
Executes the Streamlit DBA telemetry app through Streamlit's own AppTest
harness and asserts that it renders with no uncaught exceptions, regardless of
the working directory from which this script is launched.

Usage:
    python test_dashboard.py
"""
import os
import sys
from streamlit.testing.v1 import AppTest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_PATH = os.path.join(BASE_DIR, "dashboard", "app.py")


def test_dashboard_renders():
    """Run the dashboard app and return the AppTest instance."""
    at = AppTest.from_file(APP_PATH, default_timeout=60)
    at.run()
    if at.exception:
        raise AssertionError(
            "Dashboard raised uncaught exception(s): "
            + "; ".join(str(e.value) for e in at.exception)
        )
    return at


if __name__ == "__main__":
    # Windows consoles default to cp1252; the dashboard headers contain emoji,
    # so force UTF-8 output (or the print below raises UnicodeEncodeError).
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass

    try:
        at = test_dashboard_renders()
    except AssertionError as exc:
        print("[FAILURE] " + str(exc))
        sys.exit(1)

    print("CWD:", os.getcwd())
    print("APP_PATH:", APP_PATH)
    print("EXCEPTIONS:", len(at.exception))
    print("SUBHEADERS:", [s.value for s in at.subheader])
    print("METRICS:", [m.value for m in at.metric])
    print("DATAFRAMES:", len(at.dataframe))
    print("TABLES:", len(at.table))
    print("[SUCCESS] Dashboard rendered with no uncaught exceptions.")