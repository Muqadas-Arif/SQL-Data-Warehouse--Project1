/* ===============================================================
   DATA WAREHOUSE INITIALIZATION SCRIPT
   ---------------------------------------------------------------
   Author: Muqadas
   Purpose: To safely create a new Data Warehouse database and
            its three-tier schema architecture: Bronze, Silver, Gold.
   
   Notes:
   - If an existing database named 'DataWarehouse' exists, 
     it will be DROPPED (all data will be permanently lost).
   - The script ensures idempotency: running it multiple times 
     will not cause duplication errors.
================================================================= */
USE master;
Go

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create core schemas
CREATE SCHEMA Bronze;
GO

CREATE SCHEMA Silver;
GO

CREATE SCHEMA Gold;
GO



