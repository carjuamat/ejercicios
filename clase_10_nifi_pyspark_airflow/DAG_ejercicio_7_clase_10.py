######################################################
#
# DAG creado para el ejercicio 7 de la clase 10
# /home/hadoop/airflow/dags/DAG_ejercicio_7_clase_10.py
#
######################################################

# Importamos librerias

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import os  # <-- agregado para construir PATH

# Definimos los argumentos por defecto del DAG
args = {
    "owner": "airflow",
}

with DAG(
    dag_id="ingest-transform-6",
    default_args=args,
    schedule_interval="0 0 * * *",   # diario a medianoche
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["ingest", "transform"],
) as dag:

    inicio_proceso = DummyOperator(task_id="inicio_proceso")

    finaliza_proceso = DummyOperator(task_id="finaliza_proceso")
    
    # Tareas Ingest - Transform - Load (sin cambios)

    with TaskGroup("ETL", tooltip="Tareas de Extracción, Transformacion y Carga de Datos") as process_group:
        products_sold = BashOperator(
            task_id='ETL_titanic_clean',
            bash_command="""
                set -e
                /home/hadoop/spark/bin/spark-submit \
                  --master local[*] \
                  --deploy-mode client \
                  --files /home/hadoop/hive/conf/hive-site.xml \
                  /home/hadoop/scripts/transform_load_6.py
            """,
        )
    
    # Inicio del flujo de tareas ETL
    inicio_proceso >> process_group

    # Finalizacion del proceso de tareas
    process_group >> finaliza_proceso
