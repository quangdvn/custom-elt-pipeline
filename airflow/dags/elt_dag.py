import subprocess
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'start_date': datetime(2022, 4, 1),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}


def run_elt_script():
  script_path = "/opt/airflow/elt/elt_script.py"
  result = subprocess.run(["python", script_path], capture_output=True, text=True)
  if result.returncode != 0:
    raise Exception(f"ELT script failed: {result.stderr}")

  print(result.stdout)


dag = DAG('elt_dag',
          default_args=default_args,
          description='ELT DAG',
          start_date=datetime(2024, 5, 6),
          catchup=False
          # schedule_interval=timedelta(days=1)
          )

task1 = PythonOperator(
    task_id='run_elt_script',
    python_callable=run_elt_script,
    dag=dag
)

task2 = DockerOperator(
    task_id='dbt_run',
    image='ghcr.io/dbt-labs/dbt-postgres:1.6.2',
    command=[
        'run',
        '--profiles-dir',
        '/root',
        '--project-dir',
        '/opt/dbt',
        '--full-refresh'],
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        Mount(source='/Users/quangdvn/Desktop/hello_python/custom-elt/custom_postgres',
              target='/opt/dbt', type='bind'),
        Mount(source='/Users/quangdvn/.dbt',
              target='/root', type='bind')
    ],
    dag=dag
)

task1 >> task2
