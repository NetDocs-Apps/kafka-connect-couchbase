package com.couchbase.connect.kafka.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * Unit tests for the JsonPropertyExtractor class.
 * These tests cover various scenarios to ensure the correct functionality
 * of the JSON property extraction process.
 */
class JsonPropertyExtractorTests {

    /**
     * Test extraction of simple properties from a flat JSON object.
     * This test covers extraction of string, integer, double, and boolean values.
     */
    @Test
    void testSimpleJson() throws Exception {
        String json = "{\"name\":\"John\",\"age\":30,\"height\":1.75,\"isStudent\":false}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("name", "age", "height", "isStudent"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(4, result.size());
        assertEquals("John", result.get("name"));
        assertEquals(30, result.get("age"));
        assertEquals(1.75, result.get("height"));
        assertEquals(false, result.get("isStudent"));
    }

    /**
     * Test extraction of properties from nested JSON objects.
     * This test verifies that the extractor can handle dot notation for nested
     * properties.
     */
    @Test
    void testNestedJson() throws Exception {
        String json = "{\"person\":{\"name\":\"Alice\",\"age\":25,\"isEmployee\":true},\"city\":\"New York\"}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("person.name", "person.isEmployee", "city"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(3, result.size());
        assertEquals("Alice", result.get("person.name"));
        assertEquals(true, result.get("person.isEmployee"));
        assertEquals("New York", result.get("city"));
    }

    /**
     * Test extraction of properties from JSON arrays.
     * This test checks if the extractor can handle array indexing and mixed-type
     * arrays.
     */
    @Test
    void testArrayJson() throws Exception {
        String json = "{\"numbers\":[1,2,3],\"flags\":[true,false,true],\"mixed\":[1,\"two\",3.14,false]}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("numbers[1]", "flags[2]", "mixed[2]"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(3, result.size());
        assertEquals(2, result.get("numbers[1]"));
        assertEquals(true, result.get("flags[2]"));
        assertEquals(3.14, result.get("mixed[2]"));
    }

    /**
     * Test extraction of properties from a complex JSON structure.
     * This test verifies handling of nested objects, arrays, and various data
     * types.
     */
    @Test
    void testComplexJson() throws Exception {
        String json = "{\"person\":{\"name\":\"Bob\",\"age\":40,\"address\":{\"city\":\"London\",\"zipCode\":12345}},\"scores\":[9.5,8.0,9.0]}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("person.name", "person.address", "scores[1]"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(3, result.size());
        assertEquals("Bob", result.get("person.name"));
        assertEquals(8.0, result.get("scores[1]"));

        @SuppressWarnings("unchecked")
        Map<String, Object> address = (Map<String, Object>) result.get("person.address");
        assertNotNull(address);
        assertEquals(2, address.size());
        assertEquals("London", address.get("city"));
        assertEquals(12345, address.get("zipCode"));
    }

    /**
     * Test extraction from an empty JSON object.
     * This test ensures that the extractor handles empty JSON gracefully.
     */
    @Test
    void testEmptyJson() throws Exception {
        String json = "{}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("name", "age"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertTrue(result.isEmpty());
    }

    /**
     * Test extraction of non-existent properties.
     * This test verifies that the extractor correctly handles requests for
     * properties that don't exist in the JSON.
     */
    @Test
    void testNonExistentProperties() throws Exception {
        String json = "{\"name\":\"John\",\"age\":30}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("email", "phone"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertTrue(result.isEmpty());
    }

    /**
     * Test extraction of null values.
     * This test ensures that the extractor correctly handles null values in the
     * JSON.
     */
    @Test
    void testNullValues() throws Exception {
        String json = "{\"name\":\"John\",\"age\":null,\"address\":null}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("name", "age", "address"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(3, result.size());
        assertEquals("John", result.get("name"));
        assertNull(result.get("age"));
        assertNull(result.get("address"));
    }

    /**
     * Test early termination optimization with large JSON document.
     * This test verifies that the extractor stops parsing once all desired
     * properties are found, even if there's a lot more data in the JSON.
     * This is critical for performance when extracting a few fields from
     * very large documents.
     */
    @Test
    void testEarlyTerminationWithLargeDocument() throws Exception {
        // Create a JSON with desired property early, followed by massive unused data
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"targetField\":\"found\",");

        // Add 1000 large unused fields after the target
        for (int i = 0; i < 1000; i++) {
            jsonBuilder.append("\"unused").append(i).append("\":{");
            jsonBuilder.append("\"data\":\"").append("x".repeat(1000)).append("\",");
            jsonBuilder.append("\"nested\":{\"deep\":\"value\"}");
            jsonBuilder.append("}");
            if (i < 999) {
                jsonBuilder.append(",");
            }
        }
        jsonBuilder.append("}");

        String json = jsonBuilder.toString();
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("targetField"));

        long startTime = System.currentTimeMillis();
        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);
        long duration = System.currentTimeMillis() - startTime;

        assertEquals(1, result.size());
        assertEquals("found", result.get("targetField"));

        // With early termination, this should complete very quickly (< 100ms)
        // Without it, parsing all 1000 large objects would take much longer
        assertTrue(duration < 100, "Extraction took " + duration + "ms, expected < 100ms with early termination");
    }

    /**
     * Test early termination with array elements.
     * This test verifies that the extractor efficiently handles arrays when
     * only specific indices are needed.
     */
    @Test
    void testEarlyTerminationWithArrays() throws Exception {
        // Create JSON with target array element early, followed by many unused elements
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"items\":[");

        // Add target element at index 0
        jsonBuilder.append("{\"id\":\"target\",\"value\":123}");

        // Add 1000 large unused array elements
        for (int i = 1; i < 1001; i++) {
            jsonBuilder.append(",{\"id\":\"unused").append(i).append("\",");
            jsonBuilder.append("\"largeData\":\"").append("y".repeat(1000)).append("\"}");
        }
        jsonBuilder.append("]}");

        String json = jsonBuilder.toString();
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("items[0]"));

        long startTime = System.currentTimeMillis();
        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);
        long duration = System.currentTimeMillis() - startTime;

        assertEquals(1, result.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> item = (Map<String, Object>) result.get("items[0]");
        assertNotNull(item);
        assertEquals("target", item.get("id"));
        assertEquals(123, item.get("value"));

        // Should complete quickly with early termination
        assertTrue(duration < 100, "Extraction took " + duration + "ms, expected < 100ms with early termination");
    }

    /**
     * Test that skipping works correctly for nested structures.
     * This verifies that when we don't need a nested object or array,
     * we skip it entirely without parsing its contents.
     */
    @Test
    void testSkippingNestedStructures() throws Exception {
        String json = "{" +
                "\"needed\":\"value\"," +
                "\"skipThis\":{" +
                "\"deep\":{" +
                "\"nested\":{" +
                "\"structure\":[1,2,3,{\"more\":\"data\"}]" +
                "}" +
                "}" +
                "}," +
                "\"alsoNeeded\":42" +
                "}";

        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Set<String> desiredProperties = new HashSet<>(Arrays.asList("needed", "alsoNeeded"));

        Map<String, Object> result = JsonPropertyExtractor.extract(inputStream, desiredProperties);

        assertEquals(2, result.size());
        assertEquals("value", result.get("needed"));
        assertEquals(42, result.get("alsoNeeded"));
        // Verify that skipThis was not parsed
        assertFalse(result.containsKey("skipThis"));
    }
}
