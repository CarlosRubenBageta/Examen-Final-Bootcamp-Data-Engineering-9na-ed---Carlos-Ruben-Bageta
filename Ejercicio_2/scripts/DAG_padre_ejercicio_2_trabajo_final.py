#################################################################
#
# DAG padre creado para el ejercicio 2 Trabajo Final
# /home/hadoop/airflow/dags/DAG_padre_ejercicio_2_trabajo_final.py
#
#################################################################

# Importamos librerias

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Definimos los argumentos por defecto del DAG
args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dag_padre",
    default_args=args,
    schedule_interval="0 0 * * *",   
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["ingest"],
) as dag:

    inicio_proceso = DummyOperator(task_id="inicio_proceso")

    
    # Tareas Ingest 

    ingest = BashOperator(
        task_id="ingest",
        bash_command="""
            set -e
            /bin/bash /home/hadoop/scripts/ingest_ejercicio_2.sh
        """,
    )

    # Disparar el DAG hijo

    trigger_target = TriggerDagRunOperator(
        task_id="trigger_target",
        trigger_dag_id="dag_hijo",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=False,
    #   poke_interval=30,
    )

    
    # Inicio del flujo de tareas Extraccion y disparo del DAG hijo

    inicio_proceso >> ingest >> trigger_target