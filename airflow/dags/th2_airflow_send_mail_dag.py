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