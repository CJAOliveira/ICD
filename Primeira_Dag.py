from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import subprocess


def execute_my_script():
    script_path = os.path.join(
        os.environ['AIRFLOW_HOME'], 'scripts', 'ingestao_dados.py')
    subprocess.call(['python', script_path])
    print("funciona")


dag = DAG('INGESTAO_DADOS_EXAMES', description='NORMALIZAR_DADOS',
          schedule_interval='0 12 * * *',
          start_date=datetime(2023, 1, 1), catchup=False)

task = PythonOperator(
    task_id='my_task', python_callable=execute_my_script, dag=dag)


task
