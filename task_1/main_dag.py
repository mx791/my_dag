import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.decorators import task_group
import json

BASE_DIR = "/opt/airflow/dags"
dependancy_graph = json.load(open(BASE_DIR + "/task_1/data.json"))

dag = DAG(
    dag_id="load_json",
    start_date=datetime.datetime(2023,10,18),
    #schedule="@daily",
)

setup = SSHOperator(task_id="setup", dag=dag, ssh_conn_id="ssh_server", command="hostname")
end = SSHOperator(task_id="end", dag=dag, ssh_conn_id="ssh_server", command="hostname")

def lauch_etl(etl):
    def main():
        print("je lance l'etl " + etl)
    return main

def lauch_model(model):
    def main():
        print("je lance le modèle " + model)
    return main

def create_etl(name, dag=dag):
    #return PythonOperator(task_id=name, dag=dag, python_callable=lauch_etl(name))
    return SSHOperator(task_id=name, dag=dag, ssh_conn_id="ssh_server", command="hostname")


def create_model(name, dag=dag):
    return PythonOperator(task_id=name, dag=dag, python_callable=lauch_model(name))


names_to_objects = {}
for modl in dependancy_graph:
    if modl not in names_to_objects:
        names_to_objects[modl] = create_model(modl)
    for etl in dependancy_graph[modl]:
        if etl not in names_to_objects:
            names_to_objects[etl] = create_etl(etl)

        names_to_objects[etl] >> names_to_objects[modl]
        setup >> names_to_objects[etl]
        names_to_objects[modl] >> end

    
