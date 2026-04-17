-- <copyright file="OPENXML_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The XML_JSON_SIMPLE_UDF() function converts a parsed XML VARIANT into a normalized JSON object.
-- PARAMETERS:
--     INPUT: XML (VARIANT) - The XML content parsed as a VARIANT.
-- RETURNS:
--     OBJECT. A normalized JSON object representing the XML structure.
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.XML_JSON_SIMPLE_UDF(XML VARIANT)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
function toNormalJSON(xmlJSON) {
    var finalres = {};
    var name=xmlJSON['@'];
    var res = {};
    finalres[name] = res;
for(var key in xmlJSON)
    {
        if (key == "@")
        {
            res["$name"] = xmlJSON["@"];
}
        else if (key == "$") {
            continue;
}
        else if (key.startsWith("@"))
        {
            // This is an attribute
            res[key]=xmlJSON[key];
}
        else
        {
            var elements = xmlJSON['$']
            var value = xmlJSON[key];
            res[key] = [];
            if (Array.isArray(value))
            {
                for(var elementKey in value)
                {
                    var currentElement = elements[elementKey];
                    var fixedElement = toNormalJSON(currentElement);
                    res[key].push(fixedElement);
}
            }
            else if (value === 0)
            {
                var fixedElement = toNormalJSON(elements);
                res[key].push(fixedElement);
}
        }
    }
    return finalres;
}
return toNormalJSON(XML);
$$;

-- =========================================================================================================
-- Description: The OPENXML_UDF() function returns a subdataset from the XML
-- read to query it.
-- PARAMETERS:
--     INPUT: The XML content as a varchar and the path of the node to extract.
-- RETURNS:
--     STRING. Table with the XML processed as JSON structure, but it is a string.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.OPENXML_UDF(XML VARCHAR, PATH VARCHAR)
RETURNS TABLE(VALUE VARIANT)
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
SELECT VALUE from TABLE(FLATTEN(input => PUBLIC.XML_JSON_SIMPLE_UDF(PARSE_XML(XML)), path=>PATH))
$$;



