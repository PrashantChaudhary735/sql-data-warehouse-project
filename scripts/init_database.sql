/*
=====================================
Create Database and Schemas
=====================================
Script Purpose: 
This script creates a new database named 'DataWarehouse'. Also, the script sets up three schemas
within the database: 'bronze', 'silver' and 'gold'

*/

USE master;
GO
-- Create Database 'DataWarehouse'

CREATE DATABASE DataWarehouse;

-- Create the schemas required to create the datawarehouse

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver; 
GO

Create SCHEMA gold;
GO







