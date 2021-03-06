#This dag sets up our PSQL schema and loads the data in for the first time.
#Unlike incremental_load_dag.py, this should only run once.
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from tasks.CoupBotDE_Full_Load import create_tables, load_data_to_tables

default_args = {
    'owner': 'jlevy46',
    'start_date': datetime.today() - timedelta(days=1)
}

with DAG('Create_PSQL_Schema_And_Load_Data', 
    default_args=default_args,
    schedule_interval= '0 0 * * *'
    ) as dag:

    Create_PSQL_Tables = PythonOperator(
        task_id='createTables',
        python_callable=create_tables)

    Load_PSQL_Data = PythonOperator(
        task_id='loadDataToTables',
        python_callable=load_data_to_tables)


Create_PSQL_Tables >> Load_PSQL_Data 