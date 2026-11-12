from datetime import datetime

import pytest

from colibri_etl.transformations.silver.turbines_data_transformations import (
    load_new_snapshot,
    latest_snapshot_folder,
    sanitize_turbines_data,
)


def test_latest_snapshot_folder_returns_latest_valid_date_folder():
    folders = [
        ("20260101/", True),
        ("20260203/", True),
        ("20261299/", True),
        ("notes/", True),
        ("20270101.csv", False),
    ]

    assert latest_snapshot_folder(folders, "/snapshots") == "20260203"


def test_latest_snapshot_folder_requires_a_valid_date_folder():
    with pytest.raises(ValueError, match="No yyyymmdd subfolders found under /snapshots"):
        latest_snapshot_folder([("notes/", True), ("20260230/", True)], "/snapshots")


@pytest.mark.parametrize(
    ("latest_version", "snapshot_folder", "expected_version"),
    [
        (None, "20260203", 20260203),
        (20260202, "20260203", 20260203),
    ],
)
def test_load_new_snapshot_returns_data_and_integer_version(
    latest_version, snapshot_folder, expected_version
):
    def load_snapshot(folder):
        return f"data from {folder}"

    assert load_new_snapshot(latest_version, snapshot_folder, load_snapshot) == (
        f"data from {snapshot_folder}",
        expected_version,
    )


@pytest.mark.parametrize("latest_version", [20260203, 20260204])
def test_load_new_snapshot_skips_unchanged_or_older_version(latest_version):
    def fail_if_loaded(_):
        pytest.fail("An unchanged or older snapshot should not be loaded")

    assert load_new_snapshot(latest_version, "20260203", fail_if_loaded) is None


def test_sanitize_turbines_data_trims_and_casts_values(load_fixture):
    source = load_fixture("turbines.csv")
    rows = {row.turbine_id: row for row in sanitize_turbines_data(source).collect()}

    valid = rows["turbine-01"]
    assert valid.timestamp == datetime(2026, 2, 3, 4, 5, 6)
    assert valid.wind_speed == 12.5
    assert valid.wind_direction == 270.0
    assert valid.power_output == 8.0

    empty_values = rows["turbine-02"]
    assert empty_values.wind_speed is None
    assert empty_values.wind_direction is None
    assert empty_values.power_output is None
