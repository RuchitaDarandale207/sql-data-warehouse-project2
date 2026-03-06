/*
Create database and schemas

This scripts helps us create a database named 'DataWarehouse'.
Additionally, this script also creates 3 schemas under DataWarehouse named as 'bronze', 'silver', 'gold'.

***Warning

Please check if the database already exist in your sql server. 
*/
CREATE DATABASE DATAWAREHOUSE;

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

