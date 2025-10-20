/*
This script is for creating the database for the Data Warehouse called 'DataWarehouse', then creating a schema for
the bronze, silver, and gold layers within the database
*/

USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
