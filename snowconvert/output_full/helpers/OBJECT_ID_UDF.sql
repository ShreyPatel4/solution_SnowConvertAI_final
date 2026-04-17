-- <copyright file="OBJECT_ID_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The OBJECT_ID_TABLE_UDF() function checks if a Table with an specific name has been create before. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.TABLE_OBJECT_ID_UDF(NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
SELECT
    CASE ARRAY_SIZE(SPLIT(NAME, '.'))
        WHEN 1 THEN (SELECT TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 2 THEN (SELECT TABLE_SCHEMA || '.' || TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 3 THEN (SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(object_id) = UPPER(NAME))
    ELSE
        NULL
    END
$$;

-- =========================================================================================================
-- Description: The OBJECT_ID_VIEW_UDF() function checks if a View with an specific name has been create before. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.VIEW_OBJECT_ID_UDF(NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
SELECT
    CASE ARRAY_SIZE(SPLIT(NAME, '.'))
        WHEN 1 THEN (SELECT TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.VIEWS WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 2 THEN (SELECT TABLE_SCHEMA || '.' || TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.VIEWS WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 3 THEN (SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.VIEWS WHERE UPPER(object_id) = UPPER(NAME))
    ELSE
        NULL
    END
$$;

-- =========================================================================================================
-- Description: The OBJECT_ID_FUNCTION_UDF() function checks if a Function with an specific name has been create before. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.FUNCTION_OBJECT_ID_UDF(NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
SELECT
    CASE ARRAY_SIZE(SPLIT(NAME, '.'))
        WHEN 1 THEN (SELECT FUNCTION_NAME AS object_id FROM INFORMATION_SCHEMA.FUNCTIONS WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 2 THEN (SELECT FUNCTION_SCHEMA || '.' || FUNCTION_NAME AS object_id FROM INFORMATION_SCHEMA.FUNCTIONS WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 3 THEN (SELECT FUNCTION_CATALOG || '.' || FUNCTION_SCHEMA || '.' || FUNCTION_NAME AS object_id FROM INFORMATION_SCHEMA.FUNCTIONS WHERE UPPER(object_id) = UPPER(NAME))
    ELSE
        NULL
    END
$$;

-- =========================================================================================================
-- Description: The OBJECT_ID_PROCEDURE_UDF() function checks if a Procedure with an specific name has been create before. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.PROCEDURE_OBJECT_ID_UDF(NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
SELECT
    CASE ARRAY_SIZE(SPLIT(NAME, '.'))
        WHEN 1 THEN (SELECT PROCEDURE_NAME AS object_id FROM INFORMATION_SCHEMA.PROCEDURES WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 2 THEN (SELECT PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME AS object_id FROM INFORMATION_SCHEMA.PROCEDURES WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 3 THEN (SELECT PROCEDURE_CATALOG || '.' || PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME AS object_id FROM INFORMATION_SCHEMA.PROCEDURES WHERE UPPER(object_id) = UPPER(NAME))
    ELSE
        NULL
    END
$$;

-- =========================================================================================================
-- Description: The OBJECT_ID_CONSTRAINT_UDF() function checks if a Constraint with an specific name has been create before. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.CONSTRAINT_OBJECT_ID_UDF(NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
SELECT
    CASE ARRAY_SIZE(SPLIT(NAME, '.'))
        WHEN 1 THEN (SELECT CONSTRAINT_NAME AS object_id FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 2 THEN (SELECT CONSTRAINT_SCHEMA || '.' || CONSTRAINT_NAME AS object_id FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 3 THEN (SELECT CONSTRAINT_CATALOG || '.' || CONSTRAINT_SCHEMA || '.' || CONSTRAINT_NAME AS object_id FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE UPPER(object_id) = UPPER(NAME))
    ELSE
        NULL
    END
$$;

-- =========================================================================================================
-- Description: The OBJECT_ID_SEQUENCE_UDF() function checks if a Sequence with an specific name has been create before. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.SEQUENCE_OBJECT_ID_UDF(NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
SELECT
    CASE ARRAY_SIZE(SPLIT(NAME, '.'))
        WHEN 1 THEN (SELECT SEQUENCE_NAME AS object_id FROM INFORMATION_SCHEMA.SEQUENCES WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 2 THEN (SELECT SEQUENCE_SCHEMA || '.' || SEQUENCE_NAME AS object_id FROM INFORMATION_SCHEMA.SEQUENCES WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 3 THEN (SELECT SEQUENCE_CATALOG || '.' || SEQUENCE_SCHEMA || '.' || SEQUENCE_NAME AS object_id FROM INFORMATION_SCHEMA.SEQUENCES WHERE UPPER(object_id) = UPPER(NAME))
    ELSE
        NULL
    END
$$;

-- =========================================================================================================
-- Description: The OBJECT_ID_OBJECT_UDF() function checks if an Object with an specific name has been create before. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.OBJECT_ID_UDF(NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
SELECT
    CASE ARRAY_SIZE(SPLIT(NAME, '.'))
        WHEN 1 THEN (SELECT TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 2 THEN (SELECT TABLE_SCHEMA || '.' || TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(object_id) = UPPER(NAME))
        WHEN 3 THEN (SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(object_id) = UPPER(NAME))
    ELSE
        NULL
    END
$$;