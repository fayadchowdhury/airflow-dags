from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_minio():
    hook = S3Hook(aws_conn_id="minio")  # must match your Airflow connection ID
    bucket_name = "airflow-bucket"
    key = "test.txt"

    # Upload a simple string
    hook.load_string(
        string_data="hello",
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"Uploaded {key} to {bucket_name}")

with DAG(
    dag_id="minio_test_dag",
    start_date=days_ago(1),
    schedule=None,  # trigger manually
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )
