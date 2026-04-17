-- <copyright file="QUOTENAME_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The QUOTENAME_UDF() function makes a valid  SQL Server delimited identifier by returning a
-- Unicode string with the delimiters added.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.QUOTENAME_UDF(STR VARCHAR) 
RETURNS VARCHAR 
LANGUAGE SQL 
IMMUTABLE
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
    SELECT CONCAT('"', STR, '"')
$$;

CREATE OR REPLACE FUNCTION PUBLIC.QUOTENAME_UDF(STR VARCHAR,QUOTECHAR VARCHAR)
RETURNS VARCHAR 
LANGUAGE SQL 
IMMUTABLE
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
    SELECT 
      CASE WHEN LEN(QUOTECHAR) = 1 THEN concat(QUOTECHAR, STR,QUOTECHAR)
      ELSE CONCAT(SUBSTR(QUOTECHAR,1,1),STR,SUBSTR(QUOTECHAR,2,1))
      END CASE
$$;