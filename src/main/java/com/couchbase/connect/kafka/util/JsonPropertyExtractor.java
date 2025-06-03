package com.couchbase.connect.kafka.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for extracting specific properties from a JSON input stream.
 * This class provides memory-efficient parsing of JSON data without creating
 * objects for the entire JSON structure.
 *
 * Optimized version with early termination, path prefix checking, and improved
 * string handling for better performance on large JSON documents.
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
     * Extracts specified properties from a JSON input stream with optimizations.
     *
     * @param inputStream       The input stream containing JSON data.
     * @param desiredProperties Set of property paths to extract.
     * @return A map containing the extracted properties and their values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    public static Map<String, Object> extract(InputStream inputStream, Set<String> desiredProperties) throws Exception {
        JsonFactory factory = new JsonFactory();
        Map<String, Object> result = new HashMap<>();

        try (JsonParser parser = factory.createParser(inputStream)) {
            List<String> pathStack = new ArrayList<>();
            processJsonToken(parser, pathStack, desiredProperties, result);
        }

        return result;
    }

    /**
     * Checks if any desired property starts with the given path prefix.
     * This optimization allows us to skip entire JSON subtrees that don't contain
     * desired fields.
     */
    private static boolean hasMatchingPrefix(List<String> pathStack, Set<String> desiredProperties) {
        if (pathStack.isEmpty()) {
            return true; // Root level, always check
        }

        String currentPath = buildPath(pathStack);

        // Check if any desired property starts with the current path
        for (String prop : desiredProperties) {
            if (prop.startsWith(currentPath)) {
                return true;
            }
            // Also check if current path is a prefix of the property path
            // This handles cases where we're building up to a desired path
            if (currentPath.length() < prop.length()) {
                String propPrefix = prop.substring(0, Math.min(currentPath.length(), prop.length()));
                if (currentPath.equals(propPrefix)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Builds the current path from the path stack efficiently.
     * Handles array indices correctly (no dot before [index]).
     */
    private static String buildPath(List<String> pathStack) {
        if (pathStack.isEmpty()) {
            return "";
        }

        StringBuilder path = new StringBuilder();
        for (int i = 0; i < pathStack.size(); i++) {
            String segment = pathStack.get(i);
            if (i == 0) {
                path.append(segment);
            } else if (segment.startsWith("[")) {
                // Array index - no dot separator
                path.append(segment);
            } else {
                // Regular field - add dot separator
                path.append(".").append(segment);
            }
        }
        return path.toString();
    }

    /**
     * Recursively processes JSON tokens, extracting desired properties with
     * optimizations.
     *
     * @param parser            The JSON parser.
     * @param pathStack         Stack representing the current path in the JSON
     *                          structure.
     * @param desiredProperties Set of property paths to extract.
     * @param result            Map to store extracted properties and values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static void processJsonToken(JsonParser parser, List<String> pathStack, Set<String> desiredProperties,
            Map<String, Object> result) throws Exception {

        // Early termination: if we've found all desired properties, stop parsing
        if (result.size() == desiredProperties.size()) {
            return;
        }

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.getCurrentName();
            if (fieldName != null) {
                pathStack.add(fieldName);
                String currentPath = buildPath(pathStack);
                JsonToken token = parser.nextToken();

                if (desiredProperties.contains(currentPath)) {
                    // Found a desired field - extract its value
                    if (token == JsonToken.START_OBJECT) {
                        result.put(currentPath, parseComplexProperty(parser));
                    } else if (token == JsonToken.START_ARRAY) {
                        result.put(currentPath, parseArray(parser));
                    } else {
                        result.put(currentPath, getValueByType(parser));
                    }
                } else if (hasMatchingPrefix(pathStack, desiredProperties)) {
                    // Only descend into objects/arrays if they might contain desired fields
                    if (token == JsonToken.START_OBJECT) {
                        processJsonToken(parser, pathStack, desiredProperties, result);
                    } else if (token == JsonToken.START_ARRAY) {
                        processArray(parser, pathStack, desiredProperties, result);
                    }
                } else {
                    // Skip this subtree entirely - no desired fields here
                    skipValue(parser, token);
                }

                pathStack.remove(pathStack.size() - 1); // Pop the field name

                // Early termination check after processing each field
                if (result.size() == desiredProperties.size()) {
                    return;
                }
            }
        }
    }

    /**
     * Efficiently skips a JSON value without parsing it.
     */
    private static void skipValue(JsonParser parser, JsonToken token) throws Exception {
        if (token == JsonToken.START_OBJECT) {
            parser.skipChildren();
        } else if (token == JsonToken.START_ARRAY) {
            parser.skipChildren();
        }
        // For primitive values, no action needed - already consumed
    }

    /**
     * Processes JSON arrays, handling nested objects and arrays with optimizations.
     *
     * @param parser            The JSON parser.
     * @param pathStack         Stack representing the current path in the JSON
     *                          structure.
     * @param desiredProperties Set of property paths to extract.
     * @param result            Map to store extracted properties and values.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static void processArray(JsonParser parser, List<String> pathStack, Set<String> desiredProperties,
            Map<String, Object> result) throws Exception {

        // Early termination: if we've found all desired properties, stop parsing
        if (result.size() == desiredProperties.size()) {
            return;
        }

        int index = 0;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            String indexPath = "[" + index + "]";
            pathStack.add(indexPath);
            String currentPath = buildPath(pathStack);

            if (desiredProperties.contains(currentPath)) {
                // Found a desired array element
                if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                    result.put(currentPath, parseComplexProperty(parser));
                } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                    result.put(currentPath, parseArray(parser));
                } else {
                    result.put(currentPath, getValueByType(parser));
                }
            } else if (hasMatchingPrefix(pathStack, desiredProperties)) {
                // Only descend if this array element might contain desired fields
                if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                    processJsonToken(parser, pathStack, desiredProperties, result);
                } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                    processArray(parser, pathStack, desiredProperties, result);
                }
            } else {
                // Skip this array element entirely
                skipValue(parser, parser.getCurrentToken());
            }

            pathStack.remove(pathStack.size() - 1); // Pop the index
            index++;

            // Early termination check after processing each array element
            if (result.size() == desiredProperties.size()) {
                return;
            }
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
     * Parses a JSON array into a List for better performance and memory efficiency.
     *
     * @param parser The JSON parser.
     * @return A List representing the JSON array.
     * @throws Exception If an error occurs during JSON parsing.
     */
    private static Object parseArray(JsonParser parser) throws Exception {
        List<Object> array = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                array.add(parseComplexProperty(parser));
            } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                array.add(parseArray(parser));
            } else {
                array.add(getValueByType(parser));
            }
        }
        return array;
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
