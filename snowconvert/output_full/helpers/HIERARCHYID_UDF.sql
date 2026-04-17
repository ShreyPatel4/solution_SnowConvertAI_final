-- <copyright file="HIERARCHYID_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: UDF used to emulate the GetAncestor(n) HIERARCHYID method of Transact-SQL
-- PARAMETERS:
--     PATH: The canonical string representation of the hierarchy value.
--     LEVEL: The number of ancestor to extract form the path.
-- RETURNS:
--     STRING Representing the nth ancestor.
-- EXAMPLE:
--      1) SELECT PUBLIC.HIERARCHY_GET_ANCESTOR_UDF('/1/2/3/', 2);
--      2) SELECT PUBLIC.HIERARCHY_GET_ANCESTOR_UDF('/1/', 1);
--      3) SELECT PUBLIC.HIERARCHY_GET_ANCESTOR_UDF('/1/2/', 3);
--      Results:
--      1) '/1/'
--      2) '/'
--      3) NULL
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.HIERARCHY_GET_ANCESTOR_UDF(PATH VARCHAR, LEVEL NUMBER)
RETURNS STRING
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
    CASE 

        WHEN ARRAY_SIZE(STRTOK_TO_ARRAY(PATH, '/')) < LEVEL 
            THEN NULL 
        WHEN ARRAY_SIZE(STRTOK_TO_ARRAY(PATH, '/')) = LEVEL
            THEN '/'
        ELSE CONCAT('/', ARRAY_TO_STRING(ARRAY_SLICE(STRTOK_TO_ARRAY(PATH, '/'), 0, LEVEL * -1),'/'), '/') END
$$;

-- =========================================================================================================
-- Description: UDF used to emulate the GetLevel() HIERARCHYID method of Transact-SQL
-- PARAMETERS:
--     PATH: The canonical string representation of the hierarchy value.
-- RETURNS:
--     NUMBER Indicating the level number of the provided hierarchy.
-- EXAMPLE:
--      1) SELECT PUBLIC.HIERARCHY_GET_LEVEL_UDF('/1/2/3/4/');
--      2) SELECT PUBLIC.HIERARCHY_GET_LEVEL_UDF('/');
--      3) SELECT PUBLIC.HIERARCHY_GET_LEVEL_UDF('/1/2/3/4/5/6/');
--      Results:
--      1) 4
--      2) 0
--      3) 6
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.HIERARCHY_GET_LEVEL_UDF(PATH VARCHAR)
RETURNS NUMBER
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
    ARRAY_SIZE(STRTOK_TO_ARRAY(PATH, '/'))
$$;

-- =========================================================================================================
-- Description: UDF that converts hierarchy to binary and vice versa.
--              The conversion direction is determined by the TOBINARY flag.
-- PARAMETERS:
--     PATH:
--          When transforming to binary: The canonical string representation of the hierarchy value.
--          When transforming to hierarchy: The string representation of the binary value in hexadecimal for the hierarchy.
-- RETURNS:
--     STRING Containing the hexadecimal value that represents the hierarchy path, or the canonical string representation of the hierarchy.
-- EXAMPLE:
--      SELECT PUBLIC.HIERARCHY_PERFORM_CONVERSION_UDF('/1004000/1004300/1004320/', TRUE);
--      Result: 'f8003cbb123f000797a6e7e000f2f54880'
--      SELECT PUBLIC.HIERARCHY_PERFORM_CONVERSION_UDF('647440', FALSE);
--      Result: '/1.5/8/'
-- ========================================================================================================= 
CREATE OR REPLACE FUNCTION PUBLIC.HIERARCHY_PERFORM_CONVERSION_UDF(VALUE STRING, TOBINARY BOOLEAN)
RETURNS STRING
LANGUAGE JAVASCRIPT
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
    // Transform the encoded HEX value to its binary representation.
    function hexToBinary(hexString) {
        // Remove 0x prefix if present
        hexString = hexString.replace('0x', '');
        
        // Convert each hex digit to 4 binary digits
        let binary = '';
        for (let i = 0; i < hexString.length; i++) {
            // Convert hex digit to decimal number
            const decimal = parseInt(hexString[i], 16);
            
            // Convert decimal to 4-bit binary string with leading zeros
            let bits = decimal.toString(2);
            while (bits.length < 4) {
                bits = '0' + bits;
            }
            
            binary += bits;
        }
        
        return binary;
    }

    // Get bit mask from pattern string
    function getBitMask(pattern, isOne) {
        let result = 0n;
        for (let c of pattern) {
            result = (result << 1n) | (isOne(c) ? 1n : 0n);
        }
        return result;
    }
    
    // Encode a value using the patterns mask and offset
    function encodeValue(patternMask, patternOnes, minVal, val) {
        const expand = expandBits(patternMask, BigInt(val) - BigInt(minVal));
        const value = patternOnes | expand | 1n;

        return value;
    }
    
    // Expand bits according to pattern mask
    function expandBits(mask, value) {
        if (mask === 0n) {
            return 0n;
        }
    
        if ((mask & 1n) > 0n) {
            return (expandBits(mask >> 1n, value >> 1n) << 1n) | (value & 1n);
        }
    
        return expandBits(mask >> 1n, value) << 1n;
    }

    // Decode an encoded value and determine if it's the last value
    function decode(patternMask, encodedValue, min, isLast) {
        const decodedValue = compressBits(encodedValue, patternMask);
        
        isLast = (encodedValue & 1n) === 1n;
        return {value: Number((isLast ? decodedValue : decodedValue - 1n) + BigInt(min)), isLast};
    }
    
    // Compress bits according to pattern mask
    function compressBits(value, mask) {
        if (mask === 0n) {
            return 0n;
        }
    
        if ((mask & 1n) > 0n) {
            return (compressBits(value >> 1n, mask >> 1n) << 1n) | (value & 1n);
        }
    
        return compressBits(value >> 1n, mask >> 1n);
    }
    
    // Known patterns for SQL Server hierarchyid encoding
    const KnownPatterns = {
    
        positivePatterns: [
            {min: 0, max: 3, pattern: "01xxT", prefixBitLength: 2},
            {min: 4, max: 7, pattern: "100xxT", prefixBitLength: 3}, 
            {min: 8, max: 15, pattern: "101xxxT", prefixBitLength: 3},
            {min: 16, max: 79, pattern: "110xx0x1xxxT", prefixBitLength: 3},
            {min: 80, max: 1103, pattern: "1110xxx0xxx0x1xxxT", prefixBitLength: 4},
            {min: 1104, max: 5199, pattern: "11110xxxxx0xxx0x1xxxT", prefixBitLength: 5},
            {min: 5200, max: 4294972495, pattern: "111110xxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxxT", prefixBitLength: 6},
            {min: 4294972496, max: 281479271683151, pattern: "111111xxxxxxxxxxxxxx0xxxxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxxT", prefixBitLength: 6}
        ].map(pattern => ({
            ...pattern,
            prefixOnes: getBitMask(pattern.pattern.substring(0, pattern.prefixBitLength), (c) => c === '1'),
            patternOnes: getBitMask(pattern.pattern, (c) => c === '1'),
            patternMask: getBitMask(pattern.pattern, (c) => c == 'x'),
            bitLength: pattern.pattern.length
        })),
    
        // Negative patterns for encoding negative numbers  
        negativePatterns: [
            {min: -8, max: -1, pattern: "00111xxxT", prefixBitLength: 5},
            {min: -72, max: -9, pattern: "0010xx0x1xxxT", prefixBitLength: 4},
            {min: -4168, max: -73, pattern: "000110xxxxx0xxx0x1xxxT", prefixBitLength: 6},
            {min: -4294971464, max: -4169, pattern: "000101xxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxxT", prefixBitLength: 6},
            {min: -281479271682120, max: -4294971465, pattern: "000100xxxxxxxxxxxxxx0xxxxxxxxxxxxxxxxxxxxx0xxxxxx0xxx0x1xxxT", prefixBitLength: 6}
        ].map(pattern => ({
            ...pattern,
            prefixOnes: getBitMask(pattern.pattern.substring(0, pattern.prefixBitLength), (c) => c === '1'),
            patternOnes: getBitMask(pattern.pattern, (c) => c === '1'),
            patternMask: getBitMask(pattern.pattern, (c) => c == 'x'),
            bitLength: pattern.pattern.length
        })),
    
        // Get pattern for a specific value
        getPatternByValue: function(value) {
            const patterns = value >= 0 ? this.positivePatterns : this.negativePatterns;
            
            for (const pattern of patterns) {
                if (value >= pattern.min && value <= pattern.max) {
                    return pattern;
                }
            }
            
            return null;
        },

        // Get pattern by binary prefix
        getPatternByPrefix: function(binary) {
            const remaining = binary.length;

            if (remaining === 0) return null;
            if (remaining < 8 && parseInt(binary) == 0) return null;
    
            const firstTwoBits = binary.substring(0, 2);
    
            if (firstTwoBits === '00') {
                // Check negative patterns
                for (const pattern of this.negativePatterns) {
                    if (pattern.bitLength > remaining) break;
                    
                    if (pattern.prefixOnes === BigInt(`0b${binary.substring(0, pattern.prefixBitLength)}`)) {
                        return pattern;
                    }
                }
                throw new Error("Invalid binary string for hierarchyid - no matching negative pattern found");
            } else {
                // Check positive patterns
                for (const pattern of this.positivePatterns) {
                    if (pattern.pattern.length > remaining) break;
                    
                    if (pattern.prefixOnes === BigInt(`0b${binary.substring(0, pattern.prefixBitLength)}`)) {
                        return pattern;
                    }
                }
                throw new Error("Invalid binary string for hierarchyid - no matching positive pattern found");
            }
        }
    };

    // Counts the number of zeroes at the start of the pattern.
    function getPrefixZeroes(pattern) {
        const patternPrefix = pattern.pattern.substring(0, pattern.prefixBitLength);
        for (let i = 0; i < patternPrefix.length; i++) {
            if (patternPrefix[i] !== "0") 
            {
                return i;
            }
        }

        return patternPrefix.length;
    }

    // Pads the value with zeroes when the binary string does not fill the entire byte.
    function padWithZeroes(binaryValue) {
        const remainder = binaryValue.length % 8;
        if (remainder === 0) {
            return binaryValue;
        }

        const zeroesToAdd = 8 - (remainder);
        const targetLength = binaryValue.length + zeroesToAdd;
        return binaryValue.padEnd(targetLength, "0");
    }

    // Encode an integer value to its binary equivalent.
    function encodeIntToBinary(numValue, subtractOne) {
        const pattern = KnownPatterns.getPatternByValue(numValue);
        if (pattern === null) {
            return "";
        }

        let encodedValue = encodeValue(pattern.patternMask, pattern.patternOnes, pattern.min, numValue);
        encodedValue = subtractOne ? encodedValue - 1n : encodedValue;
        encodedValue = "0".repeat(getPrefixZeroes(pattern)) + encodedValue.toString(2);
        return encodedValue
    }

    // Encode a numeric value (can have decimal part) to its binary equivalent.
    function encodeNumValue(numberString) {
        let encoded = "";
        let parts = numberString.split(".");
        for (let j = 0; j < parts.length; j++) {
            if (parts.length > 1 && j < parts.length - 1) {
                const firstElement = BigInt(parts[j]) + 1n;
                encoded += encodeIntToBinary(firstElement, true);
            }
            else {
                const numValue = BigInt(parts[j]);
                encoded += encodeIntToBinary(numValue, false);
            }
        }

        return encoded;
    }
    
    // Write the binary representation of the hierarchyid path.
    function write(stringValue) {
        if (!stringValue) {
            return undefined;
        }
    
        let result = "";
        const parts = stringValue.split("/");
        for (let i = 0; i < parts.length; i++) {
            if (parts[i] === "") {
                continue;
            }

            result += encodeNumValue(parts[i]);
        }

        result = padWithZeroes(result);
        if (result === ''){
            result = '0';
        }

        hexResult = BigInt(`0b${result}`).toString(16);
        return hexResult.length === 1 ? "0" + hexResult : hexResult;
    }

    // Read binary representation and generate the hierarchyid path.
    function read(binaryValue) {
        if (!binaryValue) {
            return undefined;
        }
    
        let result = "/";
        if (binaryValue === '00000000'){
            return result;
        }

        let remaining = binaryValue;
        let finished = false;
        while (true) {
            const step = [];
    
            while (true) {
                const pattern = KnownPatterns.getPatternByPrefix(remaining);
    
                if (pattern === null) {
                    finished = true;
                    break;
                }
    
                const encodedValue = remaining.substring(0, pattern.bitLength);
                const {value, isLast} = decode(pattern.patternMask, BigInt(`0b${encodedValue}`), pattern.min, pattern.bitLength);
    
                step.push(value);
    
    
                remaining = remaining.substring(pattern.bitLength);
    
                if (isLast) {
                    break;
                }
    
            }
    
            if (step.length === 0) {
                break;
            }
    
            if (finished) {
                break;
            }
    
            if (step.length == 1) {
                result += step[0] + "/";
            } else {
                result += step.join(".") + "/";
            }
        }
        
        return result;
    }

    return TOBINARY ? write(VALUE) : read(hexToBinary(VALUE));
$$;

-- =========================================================================================================
-- Description: UDF used to convert a HIERARCHYID canonical string path to a binary value.
-- PARAMETERS:
--     PATH: The canonical string representation of the hierarchy value.
-- RETURNS:
--     VARBINARY Containing the hexadecimal value that represents the hierarchy path.
-- EXAMPLE:
--      SELECT PUBLIC.HIERARCHY_TO_BINARY_UDF('/1004000/1004300/1004320/');
--      Result: F8003CBB123F000797A6E7E000F2F54880
-- ========================================================================================================= 
CREATE OR REPLACE FUNCTION PUBLIC.HIERARCHY_TO_BINARY_UDF(PATH STRING)
RETURNS VARBINARY
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
    TO_BINARY(PUBLIC.HIERARCHY_PERFORM_CONVERSION_UDF(PATH, TRUE))
$$;

-- =========================================================================================================
-- Description: UDF used to convert a binary value representing a HierarchyId path to its canonical string.
-- PARAMETERS:
--     BINARYVALUE: The binary value of the HierarchyId.
-- RETURNS:
--     STRING containing the canonical representation of the HierarchyId path.
-- EXAMPLE:
--      SELECT PUBLIC.BINARY_TO_HIERARCHY_UDF(TO_BINARY('E0EC6F80'));
--      Result: '/200/15/'
-- ========================================================================================================= 
CREATE OR REPLACE FUNCTION PUBLIC.BINARY_TO_HIERARCHY_UDF(BINARYVALUE VARBINARY)
RETURNS VARCHAR
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 20,  "patch": "0" }, "attributes": {  "component": "udf",  "convertedOn": "04/17/2026",  "domain": "no-domain-provided",  "migrationid": "DZ2dAbBcX3Skb4MNB+rC+Q==" }}'
AS
$$
    PUBLIC.HIERARCHY_PERFORM_CONVERSION_UDF(TO_VARCHAR(BINARYVALUE), FALSE)
$$;