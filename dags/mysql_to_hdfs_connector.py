

# ######## imports block starts ###############

import MySQLdb
# import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta


# ################### import block ends #################
#
# x = "sqoop list-databases \
# --connect jdbc:mysql://localhost/ \
# --username root --password 1234"
#
# task1 = BashOperator(task_id= 'sqoop_incremental_import_all',bash_command=x)
# h = task1.execute()
# print(h)

# ######### getting meta data from mysql server i.e dbs list,each db tables list,pk column of db list ##########


# ######### mysql connection block starts  ##########

connection = MySQLdb.connect('localhost', 'root', '1234')

cursor = connection.cursor()
# ######### mysql connection block ends  ##########


# ########## functions for db list,

def dbs_list():
    cursor.execute("SHOW DATABASES;")

    databases_list = cursor.fetchall()
    db_list = []

    for _ in databases_list:
        db_list = db_list + list(_)
    return db_list


def tables_of_db(db):
    cursor.execute("USE {0};".format(db))
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    tables_list = []
    for i in tables:
        tables_list = list(i) + tables_list
    return tables_list


def pk_column(db,table_name):
    """returns the column on which Primary Key Constraint is set if table name is given."""
    try:
        cursor.execute("USE {0};".format(db))
        cursor.execute("show index from {0} where Key_name = 'PRIMARY';".format(table_name))
        pk_on = cursor.fetchall()
        pk_list = []
        if len(pk_on) <= 0:  # checking primary key is present
            cursor.execute("DESCRIBE {0};".format(table_name))
            pk_on = cursor.fetchall()
            for i in pk_on:
                pk_list = list(i) + pk_list
            return pk_list[0]

        else:
            for i in pk_on:
                pk_list = list(i) + pk_list
            return pk_list[4]
    except Exception as e:
        # print(e,db,table_name)
        return ""

# #######################################################

sqoop_command_increment_txt = "sqoop import --connect jdbc:mysql://localhost/{0} --username root --password 1234 --table {1} --check-column {2} --incremental append -m 1"+'\n'

sqoop_import_all_txt = """sqoop import-all-tables --connect jdbc:mysql://localhost/{0} --username root --password 1234 -m 1""" + "\n"

with open("sqoop_import_all.sh",'w+') as sqoop_import_job:
    with open('sqoop_incremental.sh', 'w+') as sqoop_increment_job:
        for db in dbs_list():
            sqoop_command_import_all = sqoop_import_all_txt.format(db)
            sqoop_import_job.writelines(sqoop_command_import_all)
            for table in tables_of_db(db):
                column = pk_column(db,table)
                sqoop_command_increment = sqoop_command_increment_txt.format(db, table, column)
                # print(sqoop_command_increment)
                sqoop_increment_job.writelines(sqoop_command_increment)



# print(len(dbs_list()))
cursor.close()
connection.close()




# ############ airflow block ########################



default_args = {
    'owner': 'pardha',
    'start_date': datetime(2016, 1, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# cwd = os.getcwd()
bash_run_import_all_file = "./sqoop_import_all.sh"

bash_run_incremental_load_file = "./sqoop_incremental.sh"

dag = DAG('mysql_to_hdfs_dag', default_args=default_args)
import_all_task1 = BashOperator(task_id="import_all_tables",bash_command=bash_run_import_all_file,dag=dag)

import_increment_task2 = BashOperator(task_id="import_incremented_data_task",
                                      bash_command=bash_run_incremental_load_file,dag=dag)

import_increment_task2.set_upstream(import_all_task1)


