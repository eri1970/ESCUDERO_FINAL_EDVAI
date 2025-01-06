from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from child_dag import process_and_load_to_hive  # Importa el DAG hijo

def parent_dag(parent_dag_name, child_dag_name, args):
    # ID del DAG Padre: 'car_parent_dag'
    dag = DAG(
        'car_parent_dag',  # ID del DAG Padre
        default_args=args,
        description="DAG Padre para orquestar la ingestión y procesamiento de archivos",
        schedule_interval=None,  # No es necesario un horario aquí, lo ejecutaremos manualmente
    )

    start = DummyOperator(
        task_id="start",
        dag=dag
    )

    # Ejecutar el archivo de ingesta (ingest_car.sh) usando el BashOperator
    ingest_files = BashOperator(
        task_id="ingest_files",
        bash_command="bash /home/hadoop/scripts/ingest_car.sh ",  # Ruta completa al archivo
        dag=dag
    )

    # SubDAG (DAG hijo) con ID 'car_child_dag'
    process_data = SubDagOperator(
        task_id="car_process_and_load_to_hive",  # ID de la tarea dentro del DAG Padre
        subdag=process_and_load_to_hive('car_parent_dag', 'car_child_dag', args),  # Pasando el nombre del DAG padre e hijo
        dag=dag
    )

    # Definir el flujo de trabajo
    start >> ingest_files >> process_data

    return dag

