-- <copyright file="THROW_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =======================================================================================================
-- Description: THROW statement raises an exception in SQL Server.
-- Parameters:
--   error_number: Is a constant or variable that represents the exception. error_number is int.
--   message:      Is a string or variable that describes the exception.
--   state:        Is a constant or variable between 0 and 255 that indicates the state to associate with the message.

-- Return:         Throws an exception in runtime.
--
-- Note:            In Snowflake a custom exception must be defined with some ID which is an integer between ]-20999, -20000[. -20001 is used by default.
-- =======================================================================================================

CREATE OR REPLACE PROCEDURE PUBLIC.THROW_UDP(ERROR_NUMBER INT, MESSAGE string, STATE INT)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
EXECUTE AS CALLER
AS
$$
DECLARE
    USER_EXCEPTION_SQL VARCHAR;
BEGIN
    USER_EXCEPTION_SQL:='DECLARE USER_EXCEPTION EXCEPTION 
        (-20001, \'\n'
        || 'ERROR NUMBER: ' || ERROR_NUMBER 
        || '\nMESSAGE: ' || MESSAGE 
        || '\nSTATE: ' || STATE || '\'); '
        || 'BEGIN' 
        || ' RAISE USER_EXCEPTION; '
        || 'END;' ;
    EXECUTE IMMEDIATE :USER_EXCEPTION_SQL;
END;
$$;