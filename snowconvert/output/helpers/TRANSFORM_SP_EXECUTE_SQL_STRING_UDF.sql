-- <copyright file="TRANSFORM_SP_EXECUTE_SQL_STRING_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The TRANSFORM_SP_EXECUTE_SQL_STRING_UDF() function returns a sql string in a valid format to run in
--              a EXECUTE IMMEDIATE statement
-- PARAMETERS:
--     _SQL_STRING the string to transform.
--     _PARAMS_DEFINITION the string where the original parameters for data binding were defined.
--     _PARAMS_VALUES the array of objects holding the corresponding values of the defined parameters.
-- RETURNS:
--     STRING in a format valid to be used in a EXECUTE IMMEDIATE statement.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.TRANSFORM_SP_EXECUTE_SQL_STRING_UDF(
    _SQL_STRING STRING, 
    _PARAMS_DEFINITION STRING,
    _PARAMS_NAMES ARRAY,
    _PARAMS_VALUES ARRAY
)
RETURNS STRING
LANGUAGE JAVASCRIPT
IMMUTABLE
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "QpydAbG7e3W+MVZDPMig5Q==" }}'
AS
$$
    const param_regex = /@\w+/i;
    const is_output_regex = /\b(OUT|OUTPUT)\b/i;
            
    function get_params_info(params_definition, params_names){
        const params_definition_list = params_definition.split(',');
        const lower_case_params_names = params_names.map(item => item.toLowerCase());
        
        let current_position = 0;
        return params_definition_list.map(param_definition => {
            const param_name = param_definition.match(param_regex)?.[0] || null;
            const is_output = is_output_regex.test(param_definition);
            let value_position = lower_case_params_names.indexOf(param_name.substring(1).toLowerCase());

            if (value_position === -1) {
                value_position = current_position;
            }

            current_position++;
            return [param_name, value_position, is_output];
        });
    }

    function format_value(value){
        switch(typeof value){
            case 'string':
                return `'${value}'`;
            case 'object':
                return value instanceof Date
                ? "TIMESTAMP '" + value.toISOString().replace('T', ' ').replace('Z', '') + "'"
                : String(value);
            default:
                return String(value);
        }
    }

    function replace_param_value(sql_string, param_name, value){
        const param_regex = new RegExp(`\\B${param_name}\\b`, 'gi');
        const formatted_value = format_value(value);
        return sql_string.replace(param_regex, formatted_value);
    }

    function replace_output_param(sql_string, param_name){
        const assign_regex = new RegExp(`${param_name}\\s*=`, 'gi');
        return sql_string.replace(assign_regex, '');
    }

    function transform_sql_string(sql_string, params_definition, params_names, params_values){
        const params_info = get_params_info(params_definition, params_names);
        let new_sql_string = sql_string;

        params_info.forEach(([param_name, value_position, is_output]) => {
            new_sql_string = is_output 
                ? replace_output_param(new_sql_string, param_name) 
                : replace_param_value(new_sql_string, param_name, params_values[value_position]);
        });

        return new_sql_string;
    }    

    return transform_sql_string(_SQL_STRING, _PARAMS_DEFINITION, _PARAMS_NAMES, _PARAMS_VALUES);
$$;