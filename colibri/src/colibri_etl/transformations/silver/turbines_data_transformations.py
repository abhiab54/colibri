import re
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import TypeVar

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, trim, when


DATE_FOLDER_PATTERN = re.compile(r"^\d{8}$")
_Snapshot = TypeVar("_Snapshot")


def latest_snapshot_folder(
    folder_entries: Iterable[tuple[str, bool]], base_path: str
) -> str:
    candidate_folders = []
    for name, is_directory in folder_entries:
        folder_name = name.rstrip("/")
        if not is_directory or not DATE_FOLDER_PATTERN.fullmatch(folder_name):
            continue
        try:
            datetime.strptime(folder_name, "%Y%m%d")
        except ValueError:
            continue
        candidate_folders.append(folder_name)

    if not candidate_folders:
        raise ValueError(f"No yyyymmdd subfolders found under {base_path}")

    return max(candidate_folders)


def load_new_snapshot(
    latest_version: int | str | None,
    snapshot_folder: str,
    load_snapshot: Callable[[str], _Snapshot],
) -> tuple[_Snapshot, int] | None:
    current_version = int(latest_version) if latest_version is not None else None
    next_version = int(snapshot_folder)
    if current_version is not None and next_version <= current_version:
        return None
    return load_snapshot(snapshot_folder), next_version


def sanitize_turbines_data(raw_turbines: DataFrame) -> DataFrame:
    return raw_turbines.select(
        to_timestamp(trim(col("timestamp").cast("string"))).alias("timestamp"),
        trim(col("turbine_id").cast("string")).alias("turbine_id"),
        when(
            trim(col("wind_speed").cast("string")) != "",
            trim(col("wind_speed").cast("string")).cast("double"),
        ).alias("wind_speed"),
        when(
            trim(col("wind_direction").cast("string")) != "",
            trim(col("wind_direction").cast("string")).cast("double"),
        ).alias("wind_direction"),
        when(
            trim(col("power_output").cast("string")) != "",
            trim(col("power_output").cast("string")).cast("double"),
        ).alias("power_output"),
    )
