Must install pandas - pip3 install pandas
and xlrd	    - pip3 install xlrd
in order to interact with xls files.

To initialize the database run the following code (found in db_setup.sql) as a psql admin:

DROP DATABASE IF EXISTS chsi_usda_ers;
CREATE DATABASE chsi_usda_ers;
DROP USER IF EXISTS chsi;
CREATE USER chsi WITH password 'chsi';
GRANT ALL PRIVILEGES ON DATABASE chsi_usda_ers TO chsi;  

load_data.py expects that the database and user exist, then create_database.sql (which has been updated) is run from the python code to drop/create the relations.