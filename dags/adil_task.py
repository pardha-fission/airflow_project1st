
from airflow import DAG
from airflow.contrib.operators.spark_sql_operator import SparkSqlOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'pardha',
    'start_date': datetime(2016, 1, 14)
}


dag = DAG('sparksql_adil', default_args=default_args)

database_query = "USE test1;"
load_data = "SELECT * FROM contacts;"
task1 = SparkSqlOperator(task_id="use_db", conn_id="mysql_default", dag=dag,sql=database_query)
task2 = SparkSqlOperator(task_id="load_data",sql=load_data,dag=dag)
print(task2)
task2.set_upstream(task1)



