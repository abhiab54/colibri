from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, max as max_, min as min_, regexp_extract, to_timestamp, to_timestamp, trim, when, when, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
import re
from datetime import datetime
from typing import Optional, Tuple

BASE_PATH = "/Volumes/dev_raw/colibri/turbines"
DATE_FOLDER_PATTERN = re.compile(r"^\d{8}$")

@dp.materialized_view(name="dev_raw.colibri.turbines_power_output_summary_24h")
def turbines_power_output_summary_24h() -> DataFrame:
    return (
        spark.read.table("dev_raw.colibri.turbines_data_clean_sanitized")
        .groupBy("turbine_id", window(col("timestamp"), "24 hours").alias("summary_window"))
        .agg(
            min_("power_output").alias("min_power_output"),
            max_("power_output").alias("max_power_output"),
            avg("power_output").alias("avg_power_output"),
        )
        .select(
            col("turbine_id"),
            col("summary_window.start").alias("window_start"),
            col("summary_window.end").alias("window_end"),
            col("min_power_output"),
            col("max_power_output"),
            col("avg_power_output"),
        )
    )

@dp.materialized_view(name="dev_raw.colibri.turbines_power_output_daily_anomalies")
def turbines_power_output_daily_anomalies() -> DataFrame:
    return spark.sql(
        """
        -- Build one row per turbine per day with its average power output
        -- and the number of raw readings contributing to that daily average.
        WITH daily_output AS (
          SELECT
            CAST(`timestamp` AS DATE) AS reading_date,
            turbine_id,
            AVG(power_output) AS daily_avg_power_output,
            COUNT(*) AS reading_count
          FROM dev_raw.colibri.turbines_data_clean_sanitized
          WHERE `timestamp` IS NOT NULL
            AND turbine_id IS NOT NULL
            AND power_output IS NOT NULL
          GROUP BY CAST(`timestamp` AS DATE), turbine_id
        ),
        -- Build each turbine's expected daily output baseline across the full
        -- observed period: mean daily output, standard deviation, and day count.
        turbine_baseline AS (
          SELECT
            turbine_id,
            AVG(daily_avg_power_output) AS expected_daily_power_output,
            STDDEV_SAMP(daily_avg_power_output) AS stddev_daily_power_output,
            COUNT(*) AS observed_days
          FROM daily_output
          GROUP BY turbine_id
        ),
        -- Compare each turbine-day to that turbine's own baseline and keep only
        -- days where output is more than 2 standard deviations from expected.
        daily_anomalies AS (
          SELECT
            d.reading_date,
            d.turbine_id,
            d.reading_count,
            d.daily_avg_power_output,
            b.expected_daily_power_output,
            b.stddev_daily_power_output,
            b.observed_days,
            d.daily_avg_power_output - b.expected_daily_power_output AS deviation_from_expected,
            CASE
              WHEN d.daily_avg_power_output > b.expected_daily_power_output + 2 * b.stddev_daily_power_output THEN 'high'
              WHEN d.daily_avg_power_output < b.expected_daily_power_output - 2 * b.stddev_daily_power_output THEN 'low'
            END AS anomaly_type
          FROM daily_output d
          INNER JOIN turbine_baseline b
            ON d.turbine_id = b.turbine_id
          WHERE b.stddev_daily_power_output IS NOT NULL
            AND ABS(d.daily_avg_power_output - b.expected_daily_power_output) > 2 * b.stddev_daily_power_output
        )
        -- Roll up the anomalous turbine-days to one summary row per turbine,
        -- including anomaly counts, date range, and largest deviation.
        SELECT
          turbine_id,
          COUNT(*) AS anomaly_day_count,
          MIN(reading_date) AS first_anomaly_date,
          MAX(reading_date) AS last_anomaly_date,
          ROUND(AVG(daily_avg_power_output), 4) AS avg_anomalous_daily_output,
          ROUND(AVG(expected_daily_power_output), 4) AS expected_daily_power_output,
          ROUND(AVG(stddev_daily_power_output), 4) AS stddev_daily_power_output,
          MAX(observed_days) AS observed_days,
          ROUND(MAX(ABS(deviation_from_expected)), 4) AS max_abs_deviation,
          MAX(reading_count) AS max_daily_readings,
          CASE
            WHEN SUM(CASE WHEN anomaly_type = 'high' THEN 1 ELSE 0 END) > 0
              AND SUM(CASE WHEN anomaly_type = 'low' THEN 1 ELSE 0 END) = 0 THEN 'high'
            WHEN SUM(CASE WHEN anomaly_type = 'low' THEN 1 ELSE 0 END) > 0
              AND SUM(CASE WHEN anomaly_type = 'high' THEN 1 ELSE 0 END) = 0 THEN 'low'
            ELSE 'mixed'
          END AS anomaly_type
        FROM daily_anomalies
        GROUP BY turbine_id
        ORDER BY anomaly_day_count DESC, max_abs_deviation DESC
        """
    )
