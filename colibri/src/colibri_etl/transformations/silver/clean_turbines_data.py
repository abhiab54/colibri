from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from colibri_etl.transformations.silver.turbines_data_transformations import (
    latest_snapshot_folder,
    load_new_snapshot,
    sanitize_turbines_data,
)

BASE_PATH = "/Volumes/dev_raw/colibri/turbines"


def get_latest_snapshot_folder(base_path: str = BASE_PATH) -> str:
    entries = ((entry.name, entry.isDir()) for entry in dbutils.fs.ls(base_path))
    return latest_snapshot_folder(entries, base_path)


def read_turbines_snapshot(snapshot_folder: str) -> DataFrame:
    snapshot_path = f"{BASE_PATH}/{snapshot_folder}"

    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(snapshot_path)
    )


def next_snapshot_and_version(
    latest_snapshot_version: int | None,
) -> tuple[DataFrame, int] | None:
    snapshot_folder = get_latest_snapshot_folder()
    return load_new_snapshot(
        latest_snapshot_version,
        snapshot_folder,
        read_turbines_snapshot,
    )


dp.create_streaming_table("dev_raw.colibri.turbines_data_clean")

dp.create_auto_cdc_from_snapshot_flow(
    target="dev_raw.colibri.turbines_data_clean",
    source=next_snapshot_and_version,
    keys=["timestamp", "turbine_id"],
    stored_as_scd_type=1,
)


@dp.materialized_view(name="dev_raw.colibri.turbines_data_clean_sanitized")
@dp.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dp.expect_or_drop("valid_turbine_id", "turbine_id IS NOT NULL AND turbine_id != ''")
@dp.expect_or_drop("castable_wind_speed", "wind_speed IS NOT NULL")
@dp.expect_or_drop("castable_wind_direction", "wind_direction IS NOT NULL")
@dp.expect_or_drop("castable_power_output", "power_output IS NOT NULL")
@dp.expect_or_drop("valid_wind_direction", "wind_direction >= 0 AND wind_direction <= 359")
def turbines_data_clean_filtered() -> DataFrame:
    raw_turbines = spark.read.table("dev_raw.colibri.turbines_data_clean")
    return sanitize_turbines_data(raw_turbines)