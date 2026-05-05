import os
import json
from datetime import datetime, time, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, desc, row_number, to_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("th2-mock-check-write-result").getOrCreate()

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/opt/spark/shared/dq/result.json")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "data"))
TBL_DATA_CHECK_PATH = os.getenv("TBL_DATA_CHECK_PATH", os.path.join(DATA_DIR, "tbl_data_check.csv"))
META_TABLE_NAMES_PATH = os.getenv("META_TABLE_NAMES_PATH", os.path.join(DATA_DIR, "meta_table_names.csv"))


def parse_cron_part(part: str, value: int) -> bool:
    if part == "*":
        return True

    for item in part.split(","):
        if "-" in item:
            start, end = item.split("-")
            if start.isdigit() and end.isdigit() and int(start) <= value <= int(end):
                return True
        elif item.isdigit() and int(item) == value:
            return True
    return False


def cron_matches_datetime(cron_expr: str, dt: datetime) -> bool:
    fields = cron_expr.strip().split()
    if len(fields) != 5:
        return False

    minute_field, hour_field, day_field, month_field, dow_field = fields
    custom_dow = (dt.weekday() + 2) % 7

    return (
        parse_cron_part(minute_field, dt.minute)
        and parse_cron_part(hour_field, dt.hour)
        and parse_cron_part(day_field, dt.day)
        and parse_cron_part(month_field, dt.month)
        and parse_cron_part(dow_field, custom_dow)
    )


def format_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def write_result(result):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Written result: {result}")


def main():
    today = datetime.now().date() - timedelta(days=1)
    start_of_today = datetime.combine(today, time.min)
    start_of_next_day = start_of_today + timedelta(days=1)
    print("start_of_today:", start_of_today)
    if not os.path.exists(TBL_DATA_CHECK_PATH):
        result = {
            "status": "OK",
            "message": "No data file",
            "no_data_tables": [],
            "error_data_tables": [],
        }
        write_result(result)
        spark.stop()
        return

    df = (
        spark.read.option("header", True)
        .csv(TBL_DATA_CHECK_PATH)
        .withColumn("CHECK_DATE", to_timestamp(col("CHECK_DATE"), "yyyy-MM-dd HH:mm:ss"))
    )

    if os.path.exists(META_TABLE_NAMES_PATH):
        meta_df = (
            spark.read.option("header", True).csv(META_TABLE_NAMES_PATH)
            .withColumnRenamed("full_tbl_schema_name", "meta_full_tbl_schema_name")
            .withColumnRenamed("data_non_exists_time", "meta_data_non_exists_time")
        )
        df = (
            df.join(meta_df, df.TBL_NAME == meta_df.meta_full_tbl_schema_name, "inner")
            .withColumn(
                "DATA_NON_EXISTS_TIME",
                coalesce(meta_df.meta_data_non_exists_time, col("DATA_NON_EXISTS_TIME")),
            )
            .drop(meta_df.meta_full_tbl_schema_name)
            .drop(meta_df.meta_data_non_exists_time)
        )
    else:
        print(f"Meta table names file not found: {META_TABLE_NAMES_PATH}. Using all tables from tbl_data_check.")

    df_today = df.filter((col("CHECK_DATE") >= start_of_today) & (col("CHECK_DATE") < start_of_next_day))
    row_count = df_today.count()

    if row_count == 0:
        result = {
            "status": "NO_DATA",
            "message": f"Spark phát hiện chưa có dữ liệu kiểm tra table trong ngày {format_date(today)}",
            "no_data_tables": [f"No data quality check records found for {format_date(today)}"],
            "error_data_tables": [],
        }
        write_result(result)
        spark.stop()
        return

    error_window = Window.partitionBy("TBL_NAME").orderBy(desc("CHECK_DATE"))
    df_errors = (
        df_today.filter(col("STATUS") != "HAS_DATA")
        .withColumn("row_num", row_number().over(error_window))
        .filter(col("row_num") == 1)
    )

    error_rows = df_errors.select(
        "TBL_NAME",
        "STATUS",
        "CHECK_DATE",
        "NOTE",
        "DATA_NON_EXISTS_TIME",
    ).collect()

    if not error_rows:
        result = {
            "status": "OK",
            "message": "Data OK",
            "no_data_tables": [],
            "error_data_tables": [],
        }
        write_result(result)
        spark.stop()
        return

    no_data_rows = [row for row in error_rows if row["STATUS"] == "NO_DATA"]
    error_data_rows = [row for row in error_rows if row["STATUS"] == "ERROR_DATA"]

    def process_rows(rows):
        unmatch_tables = []
        for row in rows:
            check_date = row["CHECK_DATE"]
            data_date = check_date - timedelta(days=1)
            cron_expr = row["DATA_NON_EXISTS_TIME"] or ""
            if not cron_matches_datetime(cron_expr, data_date):
                unmatch_tables.append(row["TBL_NAME"])
        return unmatch_tables

    unmatch_no_data = process_rows(no_data_rows)
    unmatch_error_data = process_rows(error_data_rows)

    if unmatch_error_data:
        status = "ERROR_DATA"
    elif unmatch_no_data:
        status = "NO_DATA"
    else:
        status = "OK"

    result = {
        "status": status,
        "message": f"TH2: Spark phát hiện vấn đề dữ liệu trong ngày {format_date(today)}",
        "no_data_tables": unmatch_no_data,
        "error_data_tables": unmatch_error_data,
        "timestamp": datetime.now().isoformat()
    }
    write_result(result)
    spark.stop()


if __name__ == "__main__":
    main()