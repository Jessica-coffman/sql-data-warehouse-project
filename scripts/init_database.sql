/*
=================================
Create Database and Schemas
=================================

Script Purpose:
  This script creates a new database called 'DataWarehouse' after checking to see if it already exisits. 
  If the database already exists, it is dropped and recreated. Additionally, the script sets up three schemas
  named 'bronze', 'silver', and 'gold'

WARNING:
  Running this script will drop the entire 'DataWarehoue' data base if it exisits.
  All data in the database will be permently deleted. PROCEED WITH CAUTION
  and ensure that you have proper backups before running this script
*/


--set database to master and create new datawarehouse database
USE master;
GO

  --check if 'DataWarehouse' exists. if so, drop the database
  IF EXISTS (Select 1 FROM sys.databases WHERE name = "DataWarehouse")
  BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create a database called 'DataWarehouse'
CREATE DATABASE DataWarehouse;

--set database to datawarehouse
USE DataWarehouse;

--building the schema for the bronze, silver, and gold 
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
