-- Creating DataBase 'Data_Warehouse'. If it exists then drop the database and create a new one.
DROP DATABASE IF EXISTS Data_Warehouse;
CREATE DATABASE Data_Warehouse;

USE Data_Warehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

CREATE SCHEMA logs;
GO