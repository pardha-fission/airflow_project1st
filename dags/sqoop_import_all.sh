sqoop import-all-tables --connect jdbc:mysql://localhost/information_schema --username root --password 1234 -m 1
sqoop import-all-tables --connect jdbc:mysql://localhost/cafe_nate --username root --password 1234 -m 1
sqoop import-all-tables --connect jdbc:mysql://localhost/lavu --username root --password 1234 -m 1
sqoop import-all-tables --connect jdbc:mysql://localhost/myflaskapp --username root --password 1234 -m 1
sqoop import-all-tables --connect jdbc:mysql://localhost/mysql --username root --password 1234 -m 1
sqoop import-all-tables --connect jdbc:mysql://localhost/performance_schema --username root --password 1234 -m 1
sqoop import-all-tables --connect jdbc:mysql://localhost/sys --username root --password 1234 -m 1
sqoop import-all-tables --connect jdbc:mysql://localhost/test1 --username root --password 1234 -m 1




/usr/share/cmf/uninstall-cloudera-manager.sh








sqoop job --create examplejob \
--import \
--connect jdbc:mysql://localhost/test1 \
--username root --password 1234\
--table contacts --m 1
