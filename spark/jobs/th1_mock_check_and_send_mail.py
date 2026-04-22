import os
import smtplib
from email.mime.text import MIMEText
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("th1-mock-check-send-mail").getOrCreate()

MOCK_STATUS = os.getenv("MOCK_STATUS", "OK")  # OK | NO_DATA | ERROR_DATA
SMTP_HOST = os.getenv("SMTP_HOST", "mailhog")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
MAIL_FROM = os.getenv("MAIL_FROM", "airflow@local.test")
MAIL_TO = os.getenv("MAIL_TO", "data-team@example.com")

def send_mail(subject: str, body: str):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())

print(f"MOCK_STATUS = {MOCK_STATUS}")

if MOCK_STATUS == "NO_DATA":
    send_mail(
        "[DWH_ALERT][TH1] Chưa có data",
        "TH1: Spark phát hiện chưa có dữ liệu và tự gửi mail."
    )
    print("Sent NO_DATA email from Spark")

elif MOCK_STATUS == "ERROR_DATA":
    send_mail(
        "[DWH_ALERT][TH1] Data lỗi",
        "TH1: Spark phát hiện dữ liệu lỗi hoặc thiếu dữ liệu và tự gửi mail."
    )
    print("Sent ERROR_DATA email from Spark")

else:
    print("TH1: Data OK - no email sent")

spark.stop()