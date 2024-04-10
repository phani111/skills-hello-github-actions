from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.gke import GKEDeployPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gke_deploy_pod_default_connection',
    default_args=default_args,
    description='Deploy a pod in GKE using the default connection',
    schedule_interval=timedelta(days=1),
)

deploy_pod_task = GKEDeployPodOperator(
    task_id='deploy_pod',
    project_id='your-project-id',
    location='your-gke-cluster-location',
    cluster_name='your-gke-cluster-name',
    manifest_path='/path/to/your/pod-manifest.yaml',
    dag=dag,
)
