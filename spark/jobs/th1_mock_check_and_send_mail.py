import os
import smtplib
from datetime import datetime, time, timedelta
from email.mime.text import MIMEText
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, desc, row_number, to_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("th1-mock-check-send-mail").getOrCreate()

SMTP_HOST = os.getenv("SMTP_HOST", "mailhog")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
MAIL_FROM = os.getenv("MAIL_FROM", "airflow@local.test")
MAIL_TO = os.getenv("MAIL_TO", "data-team@example.com")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "data"))
TBL_DATA_CHECK_PATH = os.getenv("TBL_DATA_CHECK_PATH", os.path.join(DATA_DIR, "tbl_data_check.csv"))
META_TABLE_NAMES_PATH = os.getenv("META_TABLE_NAMES_PATH", os.path.join(DATA_DIR, "meta_table_names.csv"))

def send_mail(subject: str, body: str):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())

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
    custom_dow = (dt.weekday() + 2) % 7  # Saturday=0, Sunday=1

    return (
        parse_cron_part(minute_field, dt.minute)
        and parse_cron_part(hour_field, dt.hour)
        and parse_cron_part(day_field, dt.day)
        and parse_cron_part(month_field, dt.month)
        and parse_cron_part(dow_field, custom_dow)
    )


def format_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def main():
    today = datetime.now().date() #- timedelta(days=5)
    start_of_today = datetime.combine(today, time.min)
    start_of_next_day = start_of_today + timedelta(days=1)
    print("today:", today)
    print("start_of_next_day:", start_of_next_day)

    if not os.path.exists(TBL_DATA_CHECK_PATH):
        print(f"Data file not found: {TBL_DATA_CHECK_PATH}")
        print("No data available to evaluate today.")
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
        send_mail(
            f"[DWH_ALERT][TH1] Chưa có dữ liệu kiểm tra table trong ngày {format_date(today)}",
            f"TH1: Spark phát hiện chưa có dữ liệu kiểm tra table trong ngày {format_date(today)}."
        )
        print("Sent missing-data alert")
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
        print(f"No error records found for {format_date(today)}. Data is OK.")
        spark.stop()
        return

    # Separate NO_DATA and ERROR_DATA
    no_data_rows = [row for row in error_rows if row["STATUS"] == "NO_DATA"]
    error_data_rows = [row for row in error_rows if row["STATUS"] == "ERROR_DATA"]

    def process_rows(rows, status_name):
        unmatch_tables = []
        for row in rows:
            check_date = row["CHECK_DATE"]
            data_date = check_date - timedelta(days=1)
            cron_expr = row["DATA_NON_EXISTS_TIME"] or ""
            if cron_matches_datetime(cron_expr, data_date):
                print(f"MATCH: {row['TBL_NAME']} missing data for {data_date.date()} is allowed by schedule {cron_expr}")
            else:
                unmatch_tables.append(row["TBL_NAME"])
                print(f"UNMATCH: {row['TBL_NAME']} missing data for {data_date.date()} does not match {cron_expr}")
        return unmatch_tables

    unmatch_no_data = process_rows(no_data_rows, "NO_DATA")
    unmatch_error_data = process_rows(error_data_rows, "ERROR_DATA")

    # Send combined email for NO_DATA and ERROR_DATA only if there are unmatch records
    if unmatch_no_data or unmatch_error_data:
        sections = []

        if unmatch_no_data:
            sections.append(
                "Status = NO_DATA\n"
                + "Danh sách bảng thiếu dữ liệu:\n"
                + "\n".join(unmatch_no_data)
            )

        if unmatch_error_data:
            sections.append(
                "Status = ERROR_DATA\n"
                + "Danh sách bảng có dữ liệu lỗi:\n"
                + "\n".join(unmatch_error_data)
            )

        subject = f"[DWH_ALERT][TH1] Data Quality Alert [{format_date(today)}]"
        body = f"TH1: Spark phát hiện vấn đề dữ liệu trong ngày {format_date(today)}.\n\n" + "\n\n".join(sections)
        send_mail(subject, body)
        print("Sent combined alert with NO_DATA and/or ERROR_DATA")

    if not unmatch_no_data and not unmatch_error_data:
        print(f"Dữ liệu trong ngày đã đủ - {format_date(today)}")
        
    spark.stop()


if __name__ == "__main__":
    main()
