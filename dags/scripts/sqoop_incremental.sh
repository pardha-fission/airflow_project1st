#!/bin/bash
source ./properties/job.properties

export 'whoami'

echo "[INFO]: Deleting directory :-----> $target_dir"

hadoop fs -rmr $target_dir

echo "[INFO]: Importing new records from $tableName"
sqoop import --connect jdbc:mysql://localhost:3306/test1 \
--username $username --password $password  \
--table $tableName -m 1 \
--target-dir $target_dir \
--incremental append \
--check-column last_modified \
--last-value $last_val \
--map-column-java order_date=String 
