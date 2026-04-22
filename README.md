# Airflow + Spark Data Quality Notification Flows

## Tổng quan logic check data quality

### Rule nghiệp vụ

1. Kiểm tra `TBL_DATA_CHECK` hôm nay có dữ liệu chưa

   * không có → cảnh báo **chưa có data**
2. Nếu có dữ liệu, lấy các record lỗi mới nhất:

   * `STATUS != 'HAS_DATA'`
3. Nếu không có record lỗi:

   * kết quả **OK**
4. Nếu có record lỗi:

   * cảnh báo **data lỗi / thiếu data**

---
# TH1: Airflow gọi Spark check data, Spark gửi mail

## Flow

```text
Airflow schedule
   -> trigger Spark job
      -> Spark query DB
      -> Spark check:
           - chưa có data hôm nay?
           - có data lỗi?
      -> Spark tự gửi email
   -> Airflow chỉ nhận trạng thái success/fail của Spark job
```

## DAG Airflow

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="th1_spark_send_mail_dag",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "mail", "th1"],
) as dag:

    run_spark_check = BashOperator(
        task_id="run_spark_check",
        bash_command="""
        docker exec \
          -e MOCK_STATUS=NO_DATA \
          -e SMTP_HOST=mailhog \
          -e SMTP_PORT=1025 \
          -e MAIL_FROM=airflow@local.test \
          -e MAIL_TO=data-team@example.com \
          spark-master \
          /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark/jobs/th1_mock_check_and_send_mail.py
        """
    )
```

## Spark job: `th1_mock_check_and_send_mail.py`

```python
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
```

---

# TH2: Airflow gọi Spark check data, Airflow gửi mail

## Flow

```text
Airflow schedule
   -> trigger Spark job
      -> Spark query DB
      -> Spark check data quality
      -> Spark ghi result.json
   -> Airflow đọc result.json
   -> Airflow gửi mail theo status
```

## DAG Airflow

```python
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator

RESULT_FILE = "/opt/airflow/shared/dq/result.json"

def branch_result(**context):
    with open(RESULT_FILE, "r", encoding="utf-8") as f:
        result = json.load(f)

    context["ti"].xcom_push(key="dq_result", value=result)

    if result["status"] == "NO_DATA":
        return "send_no_data_email"
    elif result["status"] == "ERROR_DATA":
        return "prepare_error_email"
    return "finish_ok"

def prepare_error_email(**context):
    result = context["ti"].xcom_pull(task_ids="branch_after_spark", key="dq_result")
    html = (
        f"<h3>{result['message']}</h3>"
        + "<p>Các bảng lỗi:</p><ul>"
        + "".join(f"<li>{t}</li>" for t in result.get("tables", []))
        + "</ul>"
    )
    context["ti"].xcom_push(key="error_email_body", value=html)

with DAG(
    dag_id="th2_airflow_send_mail_dag",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["airflow", "mail", "th2"],
) as dag:

    run_spark_check = BashOperator(
        task_id="run_spark_check",
        bash_command="""
        docker exec \
          -e MOCK_STATUS=ERROR_DATA \
          -e OUTPUT_FILE=/opt/spark/shared/dq/result.json \
          spark-master \
          /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark/jobs/th2_mock_check_write_result.py
        """
    )

    branch_after_spark = BranchPythonOperator(
        task_id="branch_after_spark",
        python_callable=branch_result
    )

    prepare_error_email_task = PythonOperator(
        task_id="prepare_error_email",
        python_callable=prepare_error_email
    )

    send_no_data_email = EmailOperator(
        task_id="send_no_data_email",
        to="data-team@example.com",
        subject="[DWH_ALERT][TH2] Chưa có data",
        html_content="<p>TH2: Airflow nhận kết quả từ Spark và gửi mail cảnh báo chưa có data.</p>"
    )

    send_error_data_email = EmailOperator(
        task_id="send_error_data_email",
        to="data-team@example.com",
        subject="[DWH_ALERT][TH2] Data lỗi",
        html_content="{{ ti.xcom_pull(task_ids='prepare_error_email', key='error_email_body') }}"
    )

    finish_ok = EmptyOperator(task_id="finish_ok")

    run_spark_check >> branch_after_spark
    branch_after_spark >> send_no_data_email
    branch_after_spark >> prepare_error_email_task >> send_error_data_email
    branch_after_spark >> finish_ok
```

## Spark job: `th2_mock_check_write_result.py`

```python
import os
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("th2-mock-check-write-result").getOrCreate()

MOCK_STATUS = os.getenv("MOCK_STATUS", "OK")  # OK | NO_DATA | ERROR_DATA
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/opt/spark/shared/dq/result.json")

if MOCK_STATUS == "NO_DATA":
    result = {
        "status": "NO_DATA",
        "message": "TH2: Không có dữ liệu trong ngày hôm nay.",
        "tables": []
    }
elif MOCK_STATUS == "ERROR_DATA":
    result = {
        "status": "ERROR_DATA",
        "message": "TH2: Phát hiện dữ liệu lỗi.",
        "tables": ["orders", "customers"]
    }
else:
    result = {
        "status": "OK",
        "message": "TH2: Dữ liệu bình thường.",
        "tables": []
    }

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print(f"Written result: {result}")
spark.stop()
```
![alt text](image.png)
---

# So sánh nhanh

## TH1: Spark gửi mail

* Airflow gọi Spark
* Spark check data quality
* Spark gửi mail

## TH2: Airflow gửi mail

* Airflow gọi Spark
* Spark check data quality
* Spark ghi kết quả
* Airflow gửi mail

---

# Vì sao dùng Airflow gửi mail thường tốt hơn Spark gửi mail

## 1. Đúng vai trò từng công cụ

* **Spark** mạnh về xử lý dữ liệu phân tán, transform, join lớn, validate dữ liệu khối lượng lớn.
* **Airflow** mạnh về orchestration: schedule, dependency, retry, branching, alerting.

Khi để Airflow gửi mail, mỗi công cụ làm đúng việc nó giỏi nhất.

## 2. Dễ retry và vận hành hơn

Nếu Spark vừa check dữ liệu vừa gửi mail, khi mail lỗi bạn thường phải rerun cả Spark job. Điều này tốn tài nguyên và mất thời gian.

Nếu Airflow gửi mail:

* rerun riêng task gửi mail
* không cần chạy lại Spark compute
* giảm chi phí compute
* giảm thời gian xử lý

## 3. Theo dõi trạng thái tập trung trên UI

Trong Airflow UI bạn nhìn thấy rõ:

* task check data thành công hay fail
* task gửi mail thành công hay fail
* thời gian chạy từng bước
* log từng task riêng biệt

Nếu Spark gửi mail, phần notify bị chôn trong log Spark job và khó theo dõi hơn.

## 4. Dễ mở rộng nhiều kênh cảnh báo

Hôm nay bạn gửi email, ngày mai có thể cần:

* Slack
  n- Microsoft Teams
* Webhook
* PagerDuty
* Telegram

Nếu notify nằm ở Airflow, bạn chỉ cần thay operator hoặc thêm task mới mà không sửa logic Spark.

## 5. Tách biệt business logic và notification

Spark nên trả kết quả như:

* OK
* NO_DATA
* ERROR_DATA
* WARNING

Airflow đọc kết quả và quyết định hành động phù hợp. Kiến trúc này sạch hơn, dễ test và dễ maintain.

## 6. Tái sử dụng cho nhiều pipeline

Một pattern gửi mail trong Airflow có thể tái sử dụng cho nhiều DAG khác nhau:

* ETL sales
* ETL finance
* Data quality customer
* Reconciliation jobs

Nếu mỗi Spark job tự gửi mail, code notify sẽ bị lặp lại ở nhiều nơi.

## 7. Bảo mật tốt hơn

Thông tin SMTP / webhook secret / token nên quản lý tập trung qua:

* Airflow Connections
* Variables
* Secret Backends

Thay vì hard-code hoặc truyền env vào nhiều Spark job.

## 8. Chi phí và hiệu năng tốt hơn

Spark cluster là tài nguyên đắt hơn task Python/Email của Airflow. Không nên giữ Spark job chạy lâu chỉ để render email hoặc retry SMTP.

## Kết luận ngắn

Nên ưu tiên **Spark compute, Airflow notify** vì:

* đúng trách nhiệm
* dễ vận hành
* dễ mở rộng
* dễ theo dõi
* tiết kiệm tài nguyên
* dễ bảo trì lâu dài

# Setup gửi mail (SMTP) trong Docker / Airflow / Spark

## Tổng quan

Cả Airflow và Spark đều thường gửi email thông qua **SMTP server**.
SMTP là giao thức chuẩn để gửi mail ra ngoài.

Bạn có thể dùng:

* **MailHog**: dùng local test trong Docker
* **Gmail SMTP**
* **Outlook / Office365 SMTP**
* SMTP nội bộ công ty
* SendGrid / Mailgun / Amazon SES SMTP relay

---

# 1. Local test với MailHog

## Docker Compose

```yaml
mailhog:
  image: mailhog/mailhog:latest
  container_name: mailhog
  ports:
    - "1025:1025"   # SMTP
    - "8025:8025"   # Web UI
```

## Ý nghĩa port

* **1025**: cổng SMTP để Airflow/Spark gửi mail vào
* **8025**: giao diện web xem mail đã nhận

## Truy cập UI

```text
http://localhost:8025
```

## Khi nào dùng

* dev local
* test template email
* demo pipeline

---

# 2. Setup Airflow gửi mail qua SMTP

## Trong docker-compose.yml

```yaml
environment:
  AIRFLOW__SMTP__SMTP_HOST: mailhog
  AIRFLOW__SMTP__SMTP_PORT: 1025
  AIRFLOW__SMTP__SMTP_STARTTLS: "false"
  AIRFLOW__SMTP__SMTP_SSL: "false"
  AIRFLOW__SMTP__SMTP_USER: ""
  AIRFLOW__SMTP__SMTP_PASSWORD: ""
  AIRFLOW__SMTP__SMTP_MAIL_FROM: airflow@local.test
```

## Nếu dùng Gmail

```yaml
environment:
  AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
  AIRFLOW__SMTP__SMTP_PORT: 587
  AIRFLOW__SMTP__SMTP_STARTTLS: "true"
  AIRFLOW__SMTP__SMTP_SSL: "false"
  AIRFLOW__SMTP__SMTP_USER: your_email@gmail.com
  AIRFLOW__SMTP__SMTP_PASSWORD: your_app_password
  AIRFLOW__SMTP__SMTP_MAIL_FROM: your_email@gmail.com
```

## Port phổ biến

* **25**: SMTP thường (nhiều nơi block)
* **465**: SMTP SSL
* **587**: SMTP STARTTLS (khuyên dùng)
* **1025**: local test MailHog

---

# 3. Setup Spark gửi mail bằng Python SMTP

## Ví dụ code

```python
import smtplib
from email.mime.text import MIMEText

SMTP_HOST = "mailhog"
SMTP_PORT = 1025
MAIL_FROM = "airflow@local.test"
MAIL_TO = "data-team@example.com"

msg = MIMEText("Data lỗi", "plain", "utf-8")
msg["Subject"] = "[DWH_ALERT] Data lỗi"
msg["From"] = MAIL_FROM
msg["To"] = MAIL_TO

with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
    server.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())
```

## Nếu dùng TLS

```python
with smtplib.SMTP("smtp.gmail.com", 587) as server:
    server.starttls()
    server.login("your_email@gmail.com", "app_password")
    server.sendmail(...)
```

## Nếu dùng SSL

```python
with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
    server.login(...)
    server.sendmail(...)
```

---

# 4. Khuyến nghị môi trường thật (Production)

## Không hard-code thông tin mail

Nên lưu qua:

* Airflow Connections
* Environment Variables
* Vault / Secret Manager

## Ví dụ env file

```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=alert@company.com
SMTP_PASSWORD=secret
SMTP_FROM=alert@company.com
```

---

# 5. Khi nào dùng loại nào

## MailHog

Dùng khi:

* local dev
* test email
* demo

## Gmail / Outlook

Dùng khi:

* team nhỏ
* gửi ít mail
* cần nhanh

## SMTP công ty / SES / SendGrid

Dùng khi:

* production
* gửi nhiều mail
* cần ổn định cao
* có audit / security

---

# 6. Best Practice

* Airflow gửi mail tốt hơn Spark trong đa số case
* Dùng port **587 + STARTTLS** cho production
* Dùng MailHog port **1025** cho local
* Không hard-code password trong code
* Tách template email riêng
* Test mail bằng staging trước khi production

# Khuyến nghị

* **Demo nhanh / POC:** TH1
* **Triển khai bài bản / dễ maintain:** TH2

Trong hệ thống ETL thực tế, nên ưu tiên **TH2** vì tách biệt compute và notification.
