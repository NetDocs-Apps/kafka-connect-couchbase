package com.couchbase.connect.kafka.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;

/**
 * Stress tests and edge cases for JsonPropertyExtractor optimizations.
 * These tests verify performance under extreme conditions and edge cases.
 */
public class JsonPropertyExtractorStressTest {

    @Test
    public void testVeryLargeDocumentWithEarlyFields() throws Exception {
        // Test document similar to max Couchbase document size (~20MB)
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\n");
        jsonBuilder.append("    \"type\": \"large-document\",\n");
        jsonBuilder.append("    \"guid\": \"early-field\",\n");
        jsonBuilder.append("    \"massiveData\": {\n");
        
        // Add ~1MB of nested data that should be skipped
        for (int i = 0; i < 1000; i++) {
            jsonBuilder.append("        \"section").append(i).append("\": {\n");
            jsonBuilder.append("            \"data\": \"");
            // Add 1KB of data per section
            for (int j = 0; j < 100; j++) {
                jsonBuilder.append("0123456789");
            }
            jsonBuilder.append("\",\n");
            jsonBuilder.append("            \"metadata\": [");
            for (int k = 0; k < 10; k++) {
                if (k > 0) jsonBuilder.append(",");
                jsonBuilder.append("\"item").append(k).append("\"");
            }
            jsonBuilder.append("]\n");
            jsonBuilder.append("        }");
            if (i < 999) jsonBuilder.append(",");
            jsonBuilder.append("\n");
        }
        
        jsonBuilder.append("    }\n");
        jsonBuilder.append("}");

        String[] fields = {"type", "guid"};

        long startTime = System.nanoTime();
        Map<String, Object> result = JsonPropertyExtractor.extract(
            new ByteArrayInputStream(jsonBuilder.toString().getBytes()), 
            fields
        );
        long endTime = System.nanoTime();

        assertEquals(2, result.size());
        assertEquals("large-document", result.get("type"));
        assertEquals("early-field", result.get("guid"));
        
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Very large document with early termination: " + durationMs + "ms");
        
        // Should terminate early and be very fast despite large document
        assertTrue(durationMs < 100, "Should terminate early on large document, took " + durationMs + "ms");
    }

    @Test
    public void testDeeplyNestedArraysAndObjects() throws Exception {
        // Test extremely deep nesting (50 levels)
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"root\":");
        
        // Create 50 levels of nesting
        for (int i = 0; i < 50; i++) {
            if (i % 2 == 0) {
                jsonBuilder.append("[{\"level").append(i).append("\":");
            } else {
                jsonBuilder.append("{\"nested").append(i).append("\":");
            }
        }
        
        jsonBuilder.append("\"deep-value\"");
        
        // Close all the nesting
        for (int i = 49; i >= 0; i--) {
            if (i % 2 == 0) {
                jsonBuilder.append("}]");
            } else {
                jsonBuilder.append("}");
            }
        }
        
        jsonBuilder.append("}");

        // Build the expected path
        StringBuilder pathBuilder = new StringBuilder("root");
        for (int i = 0; i < 50; i++) {
            if (i % 2 == 0) {
                pathBuilder.append("[0].level").append(i);
            } else {
                pathBuilder.append(".nested").append(i);
            }
        }
        
        String[] fields = {pathBuilder.toString()};

        long startTime = System.nanoTime();
        Map<String, Object> result = JsonPropertyExtractor.extract(
            new ByteArrayInputStream(jsonBuilder.toString().getBytes()), 
            fields
        );
        long endTime = System.nanoTime();

        assertEquals(1, result.size());
        assertEquals("deep-value", result.get(pathBuilder.toString()));
        
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Deeply nested extraction (50 levels): " + durationMs + "ms");
        
        assertTrue(durationMs < 50, "Deep nesting extraction took " + durationMs + "ms");
    }

    @Test
    public void testManyFieldsWithSparseMatches() throws Exception {
        // Test extracting many fields where only a few exist
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\n");
        
        // Add 100 fields, but we'll only look for a few that exist
        for (int i = 0; i < 100; i++) {
            jsonBuilder.append("    \"field").append(i).append("\": \"value").append(i).append("\"");
            if (i < 99) jsonBuilder.append(",");
            jsonBuilder.append("\n");
        }
        
        jsonBuilder.append("}");

        // Look for 50 fields, but only 5 exist
        String[] fields = new String[50];
        for (int i = 0; i < 50; i++) {
            if (i < 5) {
                fields[i] = "field" + (i * 20); // These exist: field0, field20, field40, field60, field80
            } else {
                fields[i] = "nonexistent" + i; // These don't exist
            }
        }

        long startTime = System.nanoTime();
        Map<String, Object> result = JsonPropertyExtractor.extract(
            new ByteArrayInputStream(jsonBuilder.toString().getBytes()), 
            fields
        );
        long endTime = System.nanoTime();

        assertEquals(5, result.size()); // Only 5 fields should be found
        assertEquals("value0", result.get("field0"));
        assertEquals("value20", result.get("field20"));
        assertEquals("value40", result.get("field40"));
        assertEquals("value60", result.get("field60"));
        assertEquals("value80", result.get("field80"));
        
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Many fields with sparse matches: " + durationMs + "ms");
        
        assertTrue(durationMs < 30, "Sparse field extraction took " + durationMs + "ms");
    }

    @Test
    public void testConcurrentExtractionSafety() throws Exception {
        // Test that the extractor is thread-safe for concurrent use
        String json = "{\n" +
                "    \"type\": \"concurrent-test\",\n" +
                "    \"data\": {\n" +
                "        \"field1\": \"value1\",\n" +
                "        \"field2\": \"value2\",\n" +
                "        \"field3\": \"value3\"\n" +
                "    },\n" +
                "    \"array\": [\"item1\", \"item2\", \"item3\"]\n" +
                "}";

        String[] fields = {"type", "data.field2", "array[1]"};
        
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future<Map<String, Object>>> futures = new ArrayList<>();
        
        // Submit 100 concurrent extraction tasks
        for (int i = 0; i < 100; i++) {
            futures.add(executor.submit(() -> {
                try {
                    return JsonPropertyExtractor.extract(
                        new ByteArrayInputStream(json.getBytes()), 
                        fields
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        
        // Verify all results are correct
        for (Future<Map<String, Object>> future : futures) {
            Map<String, Object> result = future.get(5, TimeUnit.SECONDS);
            assertEquals(3, result.size());
            assertEquals("concurrent-test", result.get("type"));
            assertEquals("value2", result.get("data.field2"));
            assertEquals("item2", result.get("array[1]"));
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        
        System.out.println("Concurrent extraction safety test passed");
    }

    @Test
    public void testPathPrefixEdgeCases() throws Exception {
        // Test edge cases in path prefix matching
        String json = "{\n" +
                "    \"a\": \"value-a\",\n" +
                "    \"ab\": \"value-ab\",\n" +
                "    \"abc\": {\n" +
                "        \"def\": \"value-abc-def\"\n" +
                "    },\n" +
                "    \"abcd\": {\n" +
                "        \"efg\": \"value-abcd-efg\"\n" +
                "    }\n" +
                "}";

        String[] fields = {"abc.def"};

        Map<String, Object> result = JsonPropertyExtractor.extract(
            new ByteArrayInputStream(json.getBytes()), 
            fields
        );

        assertEquals(1, result.size());
        assertEquals("value-abc-def", result.get("abc.def"));
        
        System.out.println("Path prefix edge cases test passed");
    }

    @Test
    public void testArrayIndexBoundaryConditions() throws Exception {
        // Test array index edge cases
        String json = "{\n" +
                "    \"emptyArray\": [],\n" +
                "    \"singleItem\": [\"only\"],\n" +
                "    \"largeArray\": [";
        
        // Create array with 1000 items
        for (int i = 0; i < 1000; i++) {
            if (i > 0) json += ",";
            json += "\"item" + i + "\"";
        }
        
        json += "]\n}";

        String[] fields = {
            "emptyArray[0]",      // Should not exist
            "singleItem[0]",      // Should exist
            "singleItem[1]",      // Should not exist
            "largeArray[0]",      // Should exist
            "largeArray[999]",    // Should exist
            "largeArray[1000]"    // Should not exist
        };

        Map<String, Object> result = JsonPropertyExtractor.extract(
            new ByteArrayInputStream(json.getBytes()), 
            fields
        );

        assertEquals(3, result.size()); // Only 3 should be found
        assertEquals("only", result.get("singleItem[0]"));
        assertEquals("item0", result.get("largeArray[0]"));
        assertEquals("item999", result.get("largeArray[999]"));
        
        // These should not be in the result
        assertFalse(result.containsKey("emptyArray[0]"));
        assertFalse(result.containsKey("singleItem[1]"));
        assertFalse(result.containsKey("largeArray[1000]"));
        
        System.out.println("Array index boundary conditions test passed");
    }
}
