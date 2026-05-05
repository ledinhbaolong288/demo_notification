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