package com.couchbase.connect.kafka.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Map;

/**
 * Test class to verify the optimizations in JsonPropertyExtractor work
 * correctly.
 */
public class JsonPropertyExtractorOptimizationTest {

    @Test
    public void testOptimizedExtraction() throws Exception {
        // Test JSON similar to directory documents
        String json = "{\n" +
                "    \"type\": \"directory\",\n" +
                "    \"guid\": \"test-guid-123\",\n" +
                "    \"repositories\": [\n" +
                "        {\"id\": \"repo1\", \"name\": \"Repository 1\"},\n" +
                "        {\"id\": \"repo2\", \"name\": \"Repository 2\"}\n" +
                "    ],\n" +
                "    \"cabinets\": [\n" +
                "        {\"id\": \"cab1\", \"name\": \"Cabinet 1\"}\n" +
                "    ],\n" +
                "    \"groups\": [\"group1\", \"group2\"],\n" +
                "    \"membership\": {\"role\": \"admin\"},\n" +
                "    \"billingCode\": \"BC123\",\n" +
                "    \"admins\": [\"admin1\", \"admin2\"],\n" +
                "    \"members\": [\"member1\", \"member2\"],\n" +
                "    \"ocrCabinet\": true,\n" +
                "    \"ocrConfiguration\": {\"enabled\": true},\n" +
                "    \"unwantedField\": \"this should be ignored\",\n" +
                "    \"largeUnwantedObject\": {\n" +
                "        \"nested\": {\n" +
                "            \"deeply\": {\n" +
                "                \"with\": {\n" +
                "                    \"many\": {\n" +
                "                        \"levels\": \"that should be skipped\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        // Extract the same 12 fields as the directory worker
        String[] fields = {
                "type", "guid", "repositories", "repository", "cabinets",
                "groups", "membership", "billingCode", "admins", "members",
                "ocrCabinet", "ocrConfiguration"
        };

        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);

        // Verify all expected fields are extracted
        assertEquals("directory", result.get("type"));
        assertEquals("test-guid-123", result.get("guid"));
        assertNotNull(result.get("repositories"));
        assertNotNull(result.get("cabinets"));
        assertNotNull(result.get("groups"));
        assertNotNull(result.get("membership"));
        assertEquals("BC123", result.get("billingCode"));
        assertNotNull(result.get("admins"));
        assertNotNull(result.get("members"));
        assertEquals(true, result.get("ocrCabinet"));
        assertNotNull(result.get("ocrConfiguration"));

        // Verify unwanted fields are not extracted
        assertFalse(result.containsKey("unwantedField"));
        assertFalse(result.containsKey("largeUnwantedObject"));

        System.out.println("Optimized extraction test passed!");
        System.out.println("Extracted " + result.size() + " fields");
    }

    @Test
    public void testEarlyTermination() throws Exception {
        // Test that extraction stops early when all fields are found
        String json = "{\n" +
                "    \"field1\": \"value1\",\n" +
                "    \"field2\": \"value2\",\n" +
                "    \"unwantedLargeObject\": {\n" +
                "        \"nested\": {\n" +
                "            \"deeply\": {\n" +
                "                \"with\": \"many levels that should be skipped\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        String[] fields = { "field1", "field2" };

        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("field1"));
        assertEquals("value2", result.get("field2"));
        assertFalse(result.containsKey("unwantedLargeObject"));

        System.out.println("Early termination test passed!");
    }

    @Test
    public void testPerformanceComparison() throws Exception {
        // Create a large JSON document for performance testing
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");

        // Add the fields we want
        jsonBuilder.append("\"type\":\"directory\",");
        jsonBuilder.append("\"guid\":\"test-guid\",");

        // Add many unwanted fields
        for (int i = 0; i < 1000; i++) {
            jsonBuilder.append("\"unwanted").append(i).append("\":\"value").append(i).append("\",");
        }

        // Add more wanted fields at the end
        jsonBuilder.append("\"repositories\":[{\"id\":\"repo1\"}],");
        jsonBuilder.append("\"cabinets\":[{\"id\":\"cab1\"}]");
        jsonBuilder.append("}");

        String json = jsonBuilder.toString();
        String[] fields = { "type", "guid", "repositories", "cabinets" };

        long startTime = System.nanoTime();
        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);
        long endTime = System.nanoTime();

        assertEquals(4, result.size());
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Performance test completed in " + durationMs + "ms");
        System.out.println("Extracted " + result.size() + " fields from JSON with 1000+ unwanted fields");

        // Should be fast due to optimizations
        assertTrue(durationMs < 100, "Extraction should be fast with optimizations");
    }
}
