import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain

HOME    = os.path.expanduser("~")
SCRIPTS = os.path.join(HOME, "projeto_lakehouse", "scripts")
PYTHON  = os.path.join(HOME, "airflow_env", "bin", "python3")

default_args = {
    'owner': 'engenharia_dados',
    'retries': 1,
    'env': {
	'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64'
    }
}

with DAG(
    dag_id='pipeline_lakehouse_completo',
    description='Pipeline: Bronze → Silver → Gold',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['lakehouse', 'pyspark'],
) as dag:

    bronze_customers = BashOperator(
        task_id='bronze_customers',
        bash_command=f'{PYTHON} {SCRIPTS}/bronze/bronze_customers.py',
    )
    bronze_orders = BashOperator(
        task_id='bronze_orders',
        bash_command=f'{PYTHON} {SCRIPTS}/bronze/bronze_orders.py',
    )
    bronze_orders_items = BashOperator(
        task_id='bronze_orders_items',
        bash_command=f'{PYTHON} {SCRIPTS}/bronze/bronze_orders_items.py',
    )

    silver_customers = BashOperator(
        task_id='silver_customers',
        bash_command=f'{PYTHON} {SCRIPTS}/silver/silver_customers.py',
    )
    silver_orders = BashOperator(
        task_id='silver_orders',
        bash_command=f'{PYTHON} {SCRIPTS}/silver/silver_orders.py',
    )
    silver_orders_items = BashOperator(
        task_id='silver_orders_items',
        bash_command=f'{PYTHON} {SCRIPTS}/silver/silver_orders_items.py',
    )

    gold_pedidos_por_cidade = BashOperator(
        task_id='gold_pedidos_por_cidade',
        bash_command=f'{PYTHON} {SCRIPTS}/gold/gold_pedidos_por_cidade.py',
    )

    chain(
        [bronze_customers, bronze_orders, bronze_orders_items],
        [silver_customers, silver_orders, silver_orders_items],
        gold_pedidos_por_cidade
    )
