-- <copyright file="FOR_XML_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The FOR_XML_UDF() function returns an object converted in XML
-- PARAMETERS:
--     OBJ to be converted.
--     ELEMENT_NAME to be given the object.
-- RETURNS:
--     STRING in format of XML.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.FOR_XML_UDF(OBJ OBJECT, ELEMENT_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$ 
    LISTAGG(XMLGET(PARSE_XML(TO_XML(
    PARSE_JSON(
      '{"' || ELEMENT_NAME || '": ' || OBJ ::VARCHAR || '}'
    )
    )), ELEMENT_NAME)::VARCHAR)
$$;

-- =========================================================================================================
-- Description: The FOR_XML_UDF() function returns an object converted in XML
-- PARAMETERS:
--     OBJ to be converted.
--     ELEMENT_NAME to be given the object.
--     ROOT_NAME indicates the root name in the XML.
-- RETURNS:
--     STRING in format of XML.
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.FOR_XML_UDF(OBJ OBJECT, ELEMENT_NAME VARCHAR, ROOT_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$ 
    '<' || ROOT_NAME || '>' || PUBLIC.FOR_XML_UDF(OBJ, ELEMENT_NAME) || '<' || ROOT_NAME || '>'
$$;