from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

def process_and_load_to_hive(parent_dag_name, child_dag_name, args):
    # Definimos el ID del DAG Hijo como 'car_child_dag'
    dag = DAG(
        'car_child_dag',  # ID del DAG Hijo
        default_args=args,
        description="DAG Hijo para procesar y cargar datos en Hive",
        schedule_interval=None,  # Ejecutar solo desde el DAG Padre
    )

    def run_car_transform():
        # Ejecutar el script car_transform.py usando subprocess
        subprocess.run(['python', '/home/hadoop/scripts/car_transform.py '], check=True)

    # Usar PythonOperator para ejecutar el script
    process_data_task = PythonOperator(
        task_id="car_process_and_load_to_hive_task",
        python_callable=run_car_transform,
        dag=dag
    )

    return dag

