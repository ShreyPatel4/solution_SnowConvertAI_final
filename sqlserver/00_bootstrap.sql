-- Phase 0: Create the Planning database and schema on SQL Server.
-- Usage:
--   sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i sqlserver/00_bootstrap.sql

IF DB_ID('Planning') IS NULL
    CREATE DATABASE Planning;
GO

USE Planning;
GO

IF SCHEMA_ID('Planning') IS NULL
    EXEC('CREATE SCHEMA Planning');
GO
