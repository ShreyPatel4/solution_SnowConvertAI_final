-- <copyright file="RAISERROR_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =======================================================================================================
-- Description: The RAISERROR_UDF throws an exception with an specific message
-- =======================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.RAISERROR_UDF(MSG_TEXT VARCHAR, SEVERITY DOUBLE, STATE DOUBLE, PARAMS ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
AS
$$
    // Handle null parameters
    if (MSG_TEXT === null || MSG_TEXT === undefined) {
        MSG_TEXT = '';
    }
    if (SEVERITY === null || SEVERITY === undefined) {
        SEVERITY = 0;
    }
    if (STATE === null || STATE === undefined) {
        STATE = 0;
    }
    if (PARAMS === null || PARAMS === undefined) {
        PARAMS = [];
    }

    /**Regex for:
     * % [[flag] [width] [. precision] [{h | l}]] type
     * - flag: -, +, 0, #, ' ' 
     * - width: number
     * - precision: number
     * - {h | l} type : d, i, o, s, u, x, X
    */
    const regex = /%(\-|\+|0|#|'')*(\*|[0-9])*(.(\*|[0-9]))*(d|i|o|s|u|x|X)/g;
    const found = MSG_TEXT.match(regex);
    let paramIndex = 0;
    
    // Only process if there are matches
    if (found !== null && found.length > 0) {
        for(let i=0; i < found.length; i++)
        {
            //Validate possible *.* expression
            if(found[i].match(/\*/g) != null)
            {
                const asterisks = found[i].match(/\*/g).length;
                const arguments = PARAMS.slice(paramIndex, paramIndex + asterisks);
                paramIndex = paramIndex + asterisks;
            }
        
            // Use empty string if PARAMS[paramIndex] is null or undefined
            const paramValue = (PARAMS[paramIndex] !== null && PARAMS[paramIndex] !== undefined) ? PARAMS[paramIndex] : '';
            MSG_TEXT = MSG_TEXT.replace(found[i], paramValue);
            paramIndex++;
        }
    }
    
    var MSG = `MESSAGE: ${MSG_TEXT}, LEVEL: ${SEVERITY}, STATE: ${STATE}`;
    if(SEVERITY <= 10) {
        return MSG;
    } else {
        throw MSG;
    }
$$;

CREATE OR REPLACE FUNCTION PUBLIC.RAISERROR_UDF(MSG_ID DOUBLE, SEVERITY DOUBLE, STATE DOUBLE, PARAMS ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
AS
$$
    // Handle null parameters
    if (MSG_ID === null || MSG_ID === undefined) {
        MSG_ID = 0;
    }
    if (SEVERITY === null || SEVERITY === undefined) {
        SEVERITY = 0;
    }
    if (STATE === null || STATE === undefined) {
        STATE = 0;
    }

    var MSG = `MESSAGE: ${MSG_ID}, LEVEL: ${SEVERITY}, STATE: ${STATE}`;
    if(SEVERITY <= 10) {
        return MSG;
    } else {
        throw MSG;
    }
$$;