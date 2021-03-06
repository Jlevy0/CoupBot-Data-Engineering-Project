#This dag orchestrates and schedules the process of loading and updating data to our PSQL server.
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from tasks.CoupBotDE_Incremental_Load import truncate_tables, update_tables

#Setting up our default arguments.
default_args = {
    'owner': 'jlevy46',
    'start_date': datetime.today() - timedelta(days=1)
}

with DAG('Update_CoupBot_PSQL_Data', 
    default_args=default_args,
    schedule_interval= '0 0 * * *'
    ) as dag:

    Truncate_PSQL_Data = PythonOperator(
        task_id='truncateTables',
        python_callable=truncate_tables)

    Update_PSQL_Data = PythonOperator(
        task_id='updateTables',
        python_callable=update_tables)


Truncate_PSQL_Data >> Update_PSQL_Data