from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import subprocess


def execute_my_script_01():
    script_path = os.path.join(
        os.environ['AIRFLOW_HOME'], 'scripts', 'importar_arquivos.py')
    subprocess.call(['python', script_path])
    print("funciona")


def execute_my_script_02():
    script_path = os.path.join(
        os.environ['AIRFLOW_HOME'], 'scripts', 'normatizacao_dados.py')
    subprocess.call(['python', script_path])
    print("funciona")


def execute_my_script_03():
    script_path = os.path.join(
        os.environ['AIRFLOW_HOME'], 'scripts', 'carga_dw.py')
    subprocess.call(['python', script_path])
    print("funciona")


dag = DAG('INGESTAO_DADOS_EXAMES_2', description='NORMALIZAR_DADOS',
          schedule_interval='0 */2 * * *',
          start_date=datetime(2023, 1, 1), catchup=False)

importar_dados_raw = PythonOperator(
    task_id='importar_dados_raw', python_callable=execute_my_script_01, dag=dag)

normatizar_dados = PythonOperator(
    task_id='normatizar_dados', python_callable=execute_my_script_02, dag=dag)

carga_dw_datalake = PythonOperator(
    task_id='carga_dw_datalake', python_callable=execute_my_script_03, dag=dag)

importar_dados_raw >> normatizar_dados >> carga_dw_datalake
