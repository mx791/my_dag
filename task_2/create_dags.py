import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task_group
import json

BASE_DIR = "/opt/airflow/dags"
dependancy_graph = json.load(open(BASE_DIR + "/task_1/data.json"))


def create_dag(model, deps):

    dag = DAG(
        dag_id="IMAD_" + model,
        start_date=datetime.datetime(2023,10,18),
        #schedule="@daily",
    )

    globals()[model] = dag

    model_task = EmptyOperator(model, dag=dag)
    for dep in deps:
        etl_task = EmptyOperator(dep, dag=dag)
        etl_task >> model_task
 

for model in dependancy_graph:
    create_dag(model, dependancy_graph[model])
    
