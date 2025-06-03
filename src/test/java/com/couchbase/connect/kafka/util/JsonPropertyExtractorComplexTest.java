package com.couchbase.connect.kafka.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.List;

/**
 * Comprehensive tests for JsonPropertyExtractor optimizations with complex
 * object shapes
 * that mirror real-world Couchbase document structures.
 */
public class JsonPropertyExtractorComplexTest {

    @Test
    public void testDeepNestedObjectsWithEarlyTermination() throws Exception {
        // Test deeply nested objects where desired fields appear early
        String json = "{\n" +
                "    \"type\": \"directory\",\n" +
                "    \"guid\": \"found-early\",\n" +
                "    \"level1\": {\n" +
                "        \"level2\": {\n" +
                "            \"level3\": {\n" +
                "                \"level4\": {\n" +
                "                    \"level5\": {\n" +
                "                        \"deepField\": \"should-be-skipped\",\n" +
                "                        \"massiveArray\": [1,2,3,4,5,6,7,8,9,10]\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        String[] fields = { "type", "guid" };

        long startTime = System.nanoTime();
        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);
        long endTime = System.nanoTime();

        assertEquals(2, result.size());
        assertEquals("directory", result.get("type"));
        assertEquals("found-early", result.get("guid"));

        // Should be very fast due to early termination
        long durationMs = (endTime - startTime) / 1_000_000;
        assertTrue(durationMs < 10, "Should terminate early, took " + durationMs + "ms");

        System.out.println("Deep nested early termination: " + durationMs + "ms");
    }

    @Test
    public void testMixedArraysAndObjects() throws Exception {
        // Test complex mix of arrays containing objects and objects containing arrays
        String json = "{\n" +
                "    \"repositories\": [\n" +
                "        {\n" +
                "            \"id\": \"repo1\",\n" +
                "            \"permissions\": [\"read\", \"write\"],\n" +
                "            \"metadata\": {\n" +
                "                \"created\": \"2023-01-01\",\n" +
                "                \"tags\": [\"prod\", \"secure\"]\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"id\": \"repo2\",\n" +
                "            \"permissions\": [\"read\"],\n" +
                "            \"metadata\": {\n" +
                "                \"created\": \"2023-02-01\",\n" +
                "                \"tags\": [\"dev\"]\n" +
                "            }\n" +
                "        }\n" +
                "    ],\n" +
                "    \"groups\": [\"admin\", \"user\"],\n" +
                "    \"unwantedComplexStructure\": {\n" +
                "        \"huge\": {\n" +
                "            \"nested\": {\n" +
                "                \"structure\": \"should be skipped\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        String[] fields = {
                "repositories[0].id",
                "repositories[1].metadata.created",
                "groups[1]"
        };

        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);

        assertEquals(3, result.size());
        assertEquals("repo1", result.get("repositories[0].id"));
        assertEquals("2023-02-01", result.get("repositories[1].metadata.created"));
        assertEquals("user", result.get("groups[1]"));

        System.out.println("Mixed arrays and objects test passed");
    }

    @Test
    public void testLargeArrayWithSparseExtraction() throws Exception {
        // Test large array where we only want specific indices
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"largeArray\":[");

        // Create array with 1000 elements
        for (int i = 0; i < 1000; i++) {
            if (i > 0)
                jsonBuilder.append(",");
            jsonBuilder.append("{\"index\":").append(i).append(",\"data\":\"item").append(i).append("\"}");
        }

        jsonBuilder.append("],\"type\":\"test\"}");

        String[] fields = {
                "largeArray[5].data",
                "largeArray[500].index",
                "largeArray[999].data",
                "type"
        };

        long startTime = System.nanoTime();
        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(jsonBuilder.toString().getBytes()),
                fields);
        long endTime = System.nanoTime();

        assertEquals(4, result.size());
        assertEquals("item5", result.get("largeArray[5].data"));
        assertEquals(500, result.get("largeArray[500].index"));
        assertEquals("item999", result.get("largeArray[999].data"));
        assertEquals("test", result.get("type"));

        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Large array sparse extraction: " + durationMs + "ms");

        // Should be reasonably fast despite large array
        assertTrue(durationMs < 100, "Large array extraction took " + durationMs + "ms");
    }

    @Test
    public void testDirectoryDocumentStructure() throws Exception {
        // Test structure similar to actual NetDocuments directory documents
        String json = "{\n" +
                "    \"type\": \"directory\",\n" +
                "    \"guid\": \"DIR-12345\",\n" +
                "    \"repositories\": [\n" +
                "        {\n" +
                "            \"id\": \"repo1\",\n" +
                "            \"name\": \"Main Repository\",\n" +
                "            \"settings\": {\n" +
                "                \"encryption\": true,\n" +
                "                \"backup\": false\n" +
                "            }\n" +
                "        }\n" +
                "    ],\n" +
                "    \"cabinets\": [\n" +
                "        {\n" +
                "            \"id\": \"cab1\",\n" +
                "            \"name\": \"Legal Documents\",\n" +
                "            \"permissions\": [\"read\", \"write\", \"delete\"]\n" +
                "        },\n" +
                "        {\n" +
                "            \"id\": \"cab2\",\n" +
                "            \"name\": \"HR Documents\",\n" +
                "            \"permissions\": [\"read\"]\n" +
                "        }\n" +
                "    ],\n" +
                "    \"groups\": [\"Lawyers\", \"HR\", \"Admins\"],\n" +
                "    \"membership\": {\n" +
                "        \"role\": \"admin\",\n" +
                "        \"permissions\": [\"all\"],\n" +
                "        \"inherited\": true\n" +
                "    },\n" +
                "    \"billingCode\": \"LEGAL-2024\",\n" +
                "    \"admins\": [\"admin1@company.com\", \"admin2@company.com\"],\n" +
                "    \"members\": [\"user1@company.com\", \"user2@company.com\"],\n" +
                "    \"ocrCabinet\": true,\n" +
                "    \"ocrConfiguration\": {\n" +
                "        \"enabled\": true,\n" +
                "        \"language\": \"en\",\n" +
                "        \"confidence\": 0.95\n" +
                "    },\n" +
                "    \"metadata\": {\n" +
                "        \"created\": \"2024-01-01T00:00:00Z\",\n" +
                "        \"modified\": \"2024-06-01T12:00:00Z\",\n" +
                "        \"version\": 5\n" +
                "    },\n" +
                "    \"largeUnusedSection\": {\n" +
                "        \"audit\": {\n" +
                "            \"logs\": [\n" +
                "                {\"timestamp\": \"2024-01-01\", \"action\": \"create\"},\n" +
                "                {\"timestamp\": \"2024-02-01\", \"action\": \"modify\"},\n" +
                "                {\"timestamp\": \"2024-03-01\", \"action\": \"access\"}\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        // Extract the same 12 fields as the actual directory worker
        String[] fields = {
                "type", "guid", "repositories", "repository", "cabinets",
                "groups", "membership", "billingCode", "admins", "members",
                "ocrCabinet", "ocrConfiguration"
        };

        long startTime = System.nanoTime();
        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);
        long endTime = System.nanoTime();

        // Verify all expected fields (note: "repository" field doesn't exist, so we get
        // 11)
        assertEquals(11, result.size());
        assertEquals("directory", result.get("type"));
        assertEquals("DIR-12345", result.get("guid"));
        assertNotNull(result.get("repositories"));
        assertNotNull(result.get("cabinets"));
        assertNotNull(result.get("groups"));
        assertNotNull(result.get("membership"));
        assertEquals("LEGAL-2024", result.get("billingCode"));
        assertNotNull(result.get("admins"));
        assertNotNull(result.get("members"));
        assertEquals(true, result.get("ocrCabinet"));
        assertNotNull(result.get("ocrConfiguration"));

        // Verify complex objects are extracted correctly
        @SuppressWarnings("unchecked")
        List<Object> repositories = (List<Object>) result.get("repositories");
        assertEquals(1, repositories.size());

        @SuppressWarnings("unchecked")
        List<Object> cabinets = (List<Object>) result.get("cabinets");
        assertEquals(2, cabinets.size());

        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Directory document extraction: " + durationMs + "ms");

        // Should be very fast with optimizations
        assertTrue(durationMs < 20, "Directory extraction took " + durationMs + "ms");
    }

    @Test
    public void testPathPrefixOptimizationWithSimilarPaths() throws Exception {
        // Test that path prefix optimization works correctly with similar but different
        // paths
        String json = "{\n" +
                "    \"user\": {\n" +
                "        \"name\": \"John\",\n" +
                "        \"email\": \"john@example.com\"\n" +
                "    },\n" +
                "    \"userProfile\": {\n" +
                "        \"avatar\": \"avatar.jpg\",\n" +
                "        \"preferences\": {\n" +
                "            \"theme\": \"dark\",\n" +
                "            \"language\": \"en\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"userData\": {\n" +
                "        \"loginCount\": 42,\n" +
                "        \"lastLogin\": \"2024-06-01\"\n" +
                "    }\n" +
                "}";

        String[] fields = {
                "user.name",
                "userProfile.preferences.theme"
        };

        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);

        assertEquals(2, result.size());
        assertEquals("John", result.get("user.name"));
        assertEquals("dark", result.get("userProfile.preferences.theme"));

        System.out.println("Similar paths test passed");
    }

    @Test
    public void testArrayOfArraysWithNestedObjects() throws Exception {
        // Test complex nested arrays containing objects with arrays
        String json = "{\n" +
                "    \"matrix\": [\n" +
                "        [\n" +
                "            {\"value\": 1, \"metadata\": [\"tag1\", \"tag2\"]},\n" +
                "            {\"value\": 2, \"metadata\": [\"tag3\"]}\n" +
                "        ],\n" +
                "        [\n" +
                "            {\"value\": 3, \"metadata\": [\"tag4\", \"tag5\", \"tag6\"]},\n" +
                "            {\"value\": 4, \"metadata\": []}\n" +
                "        ]\n" +
                "    ],\n" +
                "    \"config\": {\n" +
                "        \"enabled\": true\n" +
                "    }\n" +
                "}";

        String[] fields = {
                "matrix[0][1].value",
                "matrix[1][0].metadata[2]",
                "config.enabled"
        };

        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);

        assertEquals(3, result.size());
        assertEquals(2, result.get("matrix[0][1].value"));
        assertEquals("tag6", result.get("matrix[1][0].metadata[2]"));
        assertEquals(true, result.get("config.enabled"));

        System.out.println("Array of arrays test passed");
    }

    @Test
    public void testSkippingLargeUnwantedSections() throws Exception {
        // Test that large unwanted sections are efficiently skipped
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\n");
        jsonBuilder.append("    \"wantedField\": \"found\",\n");
        jsonBuilder.append("    \"hugeUnwantedSection\": {\n");

        // Add a massive nested structure that should be skipped
        for (int i = 0; i < 100; i++) {
            jsonBuilder.append("        \"level").append(i).append("\": {\n");
            jsonBuilder.append("            \"data\": [");
            for (int j = 0; j < 50; j++) {
                if (j > 0)
                    jsonBuilder.append(",");
                jsonBuilder.append("{\"item\":").append(j).append("}");
            }
            jsonBuilder.append("],\n");
            jsonBuilder.append("            \"end\": \"final\"\n");
            jsonBuilder.append("        }");
            if (i < 99) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append("\n");
        }

        jsonBuilder.append("    },\n");
        jsonBuilder.append("    \"anotherWantedField\": \"also-found\"\n");
        jsonBuilder.append("}");

        String[] fields = { "wantedField", "anotherWantedField" };

        long startTime = System.nanoTime();
        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(jsonBuilder.toString().getBytes()),
                fields);
        long endTime = System.nanoTime();

        assertEquals(2, result.size());
        assertEquals("found", result.get("wantedField"));
        assertEquals("also-found", result.get("anotherWantedField"));

        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Skipping large sections: " + durationMs + "ms");

        // Should be fast due to skipping optimization
        assertTrue(durationMs < 50, "Should skip large sections efficiently, took " + durationMs + "ms");
    }

    @Test
    public void testNullAndEmptyValueHandling() throws Exception {
        // Test handling of null values, empty arrays, and empty objects
        String json = "{\n" +
                "    \"nullField\": null,\n" +
                "    \"emptyArray\": [],\n" +
                "    \"emptyObject\": {},\n" +
                "    \"arrayWithNulls\": [null, \"value\", null],\n" +
                "    \"objectWithNulls\": {\n" +
                "        \"nullProp\": null,\n" +
                "        \"validProp\": \"valid\"\n" +
                "    }\n" +
                "}";

        String[] fields = {
                "nullField",
                "emptyArray",
                "emptyObject",
                "arrayWithNulls[1]",
                "objectWithNulls.nullProp",
                "objectWithNulls.validProp"
        };

        Map<String, Object> result = JsonPropertyExtractor.extract(
                new ByteArrayInputStream(json.getBytes()),
                fields);

        assertEquals(6, result.size());
        assertNull(result.get("nullField"));
        assertNotNull(result.get("emptyArray"));
        assertNotNull(result.get("emptyObject"));
        assertEquals("value", result.get("arrayWithNulls[1]"));
        assertNull(result.get("objectWithNulls.nullProp"));
        assertEquals("valid", result.get("objectWithNulls.validProp"));

        System.out.println("Null and empty value handling test passed");
    }
}
