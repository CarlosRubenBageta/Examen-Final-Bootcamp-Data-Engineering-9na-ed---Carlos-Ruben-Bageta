#################################################################
#
# DAG hijo creado para el ejercicio 2 Trabajo Final
# /home/hadoop/airflow/dags/DAG_hijo_ejercicio_2_trabajo_final.py
#
#################################################################

# Importamos librerias

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


# Definimos los argumentos por defecto del DAG
args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dag_hijo",
    default_args=args,
    schedule_interval="0 0 * * *",   
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["transform_load"],
) as dag:

    
    # Tareas Transform - Load

    transform_load = BashOperator(
        task_id='transform_load',
        bash_command="""
            set -e
            /home/hadoop/spark/bin/spark-submit \
                --master local[*] \
                --deploy-mode client \
                --files /home/hadoop/hive/conf/hive-site.xml \
                /home/hadoop/scripts/transform_load_8.py
        """,
    )

    finaliza_proceso = DummyOperator(task_id="finaliza_proceso")

    # Finalizacion del flujo de tareas transform & load
    transform_load >> finaliza_proceso