DROP DATABASE IF EXISTS chsi_usda_ers;
CREATE DATABASE chsi_usda_ers;

DROP USER IF EXISTS chsi;
CREATE USER chsi WITH password 'chsi';

GRANT ALL PRIVILEGES ON DATABASE chsi_usda_ers TO chsi;
