package com.couchbase.connect.kafka.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for extracting specific properties from a JSON input stream.
 * This class provides memory-efficient parsing of JSON data without creating
 * objects for the entire JSON structure.
 */
public class JsonPropertyExtractor {

    /**
     * Extracts specified properties from a JSON input stream.
     * 
     * @param inputStream The input stream containing JSON data.
     * @param paths       Array of property paths to extract.
     * @return A map containing the extracted properties and their values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    public static Map<String, Object> extract(InputStream inputStream, String[] paths) throws Exception {
        Set<String> desiredProperties = new HashSet<>(Set.of(paths));
        return extract(inputStream, desiredProperties);
    }

    /**
     * Extracts specified properties from a JSON input stream.
     *
     * @param inputStream       The input stream containing JSON data.
     * @param desiredProperties Set of property paths to extract.
     * @return A map containing the extracted properties and their values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    public static Map<String, Object> extract(InputStream inputStream, Set<String> desiredProperties) throws Exception {
        JsonFactory factory = new JsonFactory();
        Map<String, Object> result = new HashMap<>();
        Set<String> remainingProperties = new HashSet<>(desiredProperties);

        try (JsonParser parser = factory.createParser(inputStream)) {
            String currentPath = "";
            processJsonToken(parser, currentPath, remainingProperties, result);
        }

        return result;
    }

    /**
     * Recursively processes JSON tokens, extracting desired properties.
     *
     * @param parser              The JSON parser.
     * @param currentPath         The current path in the JSON structure.
     * @param remainingProperties Set of property paths still to extract (modified
     *                            in place).
     * @param result              Map to store extracted properties and values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static void processJsonToken(JsonParser parser, String currentPath, Set<String> remainingProperties,
            Map<String, Object> result) throws Exception {
        JsonToken token;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT && token != null) {
            // Early termination: if we've found all desired properties, skip the rest
            if (remainingProperties.isEmpty()) {
                skipRemainingTokens(parser);
                return;
            }

            String fieldName = parser.getCurrentName();
            if (fieldName != null) {
                String newPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
                token = parser.nextToken();

                if (token == null) {
                    break;
                }

                if (remainingProperties.contains(newPath)) {
                    if (token == JsonToken.START_OBJECT) {
                        result.put(newPath, parseComplexProperty(parser));
                    } else if (token == JsonToken.START_ARRAY) {
                        result.put(newPath, parseArray(parser));
                    } else {
                        result.put(newPath, getValueByType(parser));
                    }
                    remainingProperties.remove(newPath);
                } else if (shouldDescendIntoPath(newPath, remainingProperties)) {
                    if (token == JsonToken.START_OBJECT) {
                        processJsonToken(parser, newPath, remainingProperties, result);
                    } else if (token == JsonToken.START_ARRAY) {
                        processArray(parser, newPath, remainingProperties, result);
                    }
                } else {
                    // Skip this value entirely as it's not needed
                    skipValue(parser, token);
                }
            }
        }
    }

    /**
     * Checks if we should descend into a path to find nested properties.
     *
     * @param currentPath         The current path in the JSON structure.
     * @param remainingProperties Set of property paths still to extract.
     * @return true if any remaining property starts with the current path.
     */
    private static boolean shouldDescendIntoPath(String currentPath, Set<String> remainingProperties) {
        String prefix = currentPath + ".";
        String arrayPrefix = currentPath + "[";
        for (String prop : remainingProperties) {
            if (prop.startsWith(prefix) || prop.startsWith(arrayPrefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Skips a JSON value (object, array, or primitive) without parsing it.
     * Uses Jackson's built-in skipChildren() method for efficient skipping
     * of nested structures.
     *
     * @param parser       The JSON parser.
     * @param currentToken The current token type.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static void skipValue(JsonParser parser, JsonToken currentToken) throws Exception {
        if (currentToken == JsonToken.START_OBJECT || currentToken == JsonToken.START_ARRAY) {
            // Use Jackson's built-in method for efficient skipping
            parser.skipChildren();
        }
        // For primitive values, we're already past them after nextToken()
    }

    /**
     * Skips all remaining tokens until the end of the current object.
     *
     * @param parser The JSON parser.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static void skipRemainingTokens(JsonParser parser) throws Exception {
        int depth = 1;
        while (depth > 0) {
            JsonToken token = parser.nextToken();
            if (token == null)
                break;
            if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                depth++;
            } else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                depth--;
            }
        }
    }

    /**
     * Processes JSON arrays, handling nested objects and arrays.
     *
     * @param parser              The JSON parser.
     * @param currentPath         The current path in the JSON structure.
     * @param remainingProperties Set of property paths still to extract (modified
     *                            in place).
     * @param result              Map to store extracted properties and values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static void processArray(JsonParser parser, String currentPath, Set<String> remainingProperties,
            Map<String, Object> result) throws Exception {
        int index = 0;
        JsonToken token;
        while ((token = parser.nextToken()) != JsonToken.END_ARRAY && token != null) {
            // Early termination: if we've found all desired properties, skip the rest
            if (remainingProperties.isEmpty()) {
                skipRemainingTokens(parser);
                return;
            }

            String newPath = currentPath + "[" + index + "]";

            if (remainingProperties.contains(newPath)) {
                if (token == JsonToken.START_OBJECT) {
                    result.put(newPath, parseComplexProperty(parser));
                } else if (token == JsonToken.START_ARRAY) {
                    result.put(newPath, parseArray(parser));
                } else {
                    result.put(newPath, getValueByType(parser));
                }
                remainingProperties.remove(newPath);
            } else if (shouldDescendIntoPath(newPath, remainingProperties)) {
                if (token == JsonToken.START_OBJECT) {
                    processJsonToken(parser, newPath, remainingProperties, result);
                } else if (token == JsonToken.START_ARRAY) {
                    processArray(parser, newPath, remainingProperties, result);
                }
            } else {
                // Skip this value entirely as it's not needed
                skipValue(parser, token);
            }
            index++;
        }
    }

    /**
     * Parses a complex property (object) into a Map.
     * 
     * @param parser The JSON parser.
     * @return A Map representing the complex property.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static Map<String, Object> parseComplexProperty(JsonParser parser) throws Exception {
        Map<String, Object> complexProperty = new HashMap<>();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.getCurrentName();
            parser.nextToken();
            if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                complexProperty.put(fieldName, parseComplexProperty(parser));
            } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                complexProperty.put(fieldName, parseArray(parser));
            } else {
                complexProperty.put(fieldName, getValueByType(parser));
            }
        }
        return complexProperty;
    }

    /**
     * Parses a JSON array into a List.
     * Uses ArrayList for efficient dynamic sizing without manual array copying.
     *
     * @param parser The JSON parser.
     * @return A List representing the JSON array.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static List<Object> parseArray(JsonParser parser) throws Exception {
        List<Object> list = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                list.add(parseComplexProperty(parser));
            } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                list.add(parseArray(parser));
            } else {
                list.add(getValueByType(parser));
            }
        }
        return list;
    }

    /**
     * Returns the appropriate value based on the current token type.
     * 
     * @param parser The JSON parser.
     * @return The value as String, Integer, Double, Boolean, or null.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static Object getValueByType(JsonParser parser) throws Exception {
        switch (parser.getCurrentToken()) {
            case VALUE_STRING:
                return parser.getValueAsString();
            case VALUE_NUMBER_INT:
                return parser.getValueAsInt();
            case VALUE_NUMBER_FLOAT:
                return parser.getValueAsDouble();
            case VALUE_TRUE:
                return true;
            case VALUE_FALSE:
                return false;
            case VALUE_NULL:
                return null;
            default:
                return parser.getValueAsString(); // Fallback to string for unknown types
        }
    }
}
