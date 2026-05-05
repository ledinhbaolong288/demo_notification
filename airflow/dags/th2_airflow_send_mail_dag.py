import json
import re
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

    if result.get("no_data_tables") or result.get("error_data_tables"):
        return "prepare_combined_email"
    return "finish_ok"

def prepare_combined_email(**context):
    result = context["ti"].xcom_pull(task_ids="branch_after_spark", key="dq_result")
    
    # Detect "no records" case
    is_no_records = (
        result.get("no_data_tables")
        and len(result.get("no_data_tables", [])) == 1
        and "No data quality check records found" in result.get("no_data_tables", [])[0]
    )
    
    # Extract date from message (format: "... ngày YYYY-MM-DD")
    message = result.get('message', '')
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', message)
    today = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")
    
    if is_no_records:
        # Special case for no records
        subject = f"[DWH_ALERT][TH2] Chưa có dữ liệu kiểm tra table trong ngày {today}"
        html = f"<h1>{message}</h1>"
    else:
        # Normal combined email case
        subject = f"[DWH_ALERT][TH2] Data Quality Alert [{today}]"
        sections = []

        if result.get("no_data_tables") and not is_no_records:
            sections.append(
                "<h2>Status = NO_DATA</h2>"
                + "<p>Danh sách bảng thiếu dữ liệu:</p><ul>"
                + "".join(f"<li>{t}</li>" for t in result.get("no_data_tables", []))
                + "</ul>"
            )

        if result.get("error_data_tables"):
            sections.append(
                "<h2>Status = ERROR_DATA</h2>"
                + "<p>Danh sách bảng có dữ liệu lỗi:</p><ul>"
                + "".join(f"<li>{t}</li>" for t in result.get("error_data_tables", []))
                + "</ul>"
            )

        html = (
            f"<h1>{message}</h1>"
            + "".join(sections)
        )
    
    context["ti"].xcom_push(key="combined_email_subject", value=subject)
    context["ti"].xcom_push(key="combined_email_body", value=html)


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

    prepare_combined_email_task = PythonOperator(
        task_id="prepare_combined_email",
        python_callable=prepare_combined_email
    )

    send_combined_email = EmailOperator(
        task_id="send_combined_email",
        to="data-team@example.com",
        subject="{{ ti.xcom_pull(task_ids='prepare_combined_email', key='combined_email_subject') }}",
        html_content="{{ ti.xcom_pull(task_ids='prepare_combined_email', key='combined_email_body') }}"
    )

    finish_ok = EmptyOperator(task_id="finish_ok")

    run_spark_check >> branch_after_spark
    branch_after_spark >> prepare_combined_email_task >> send_combined_email
    branch_after_spark >> finish_ok