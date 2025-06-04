package com.netdocuments.connect.kafka.handler.source;

import com.couchbase.connect.kafka.util.JsonPropertyExtractor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class to specifically test field extraction from the problematic
 * document DUCOT-RMXI2JXY
 * that consistently triggers the Kafka Connect framework hang.
 */
public class ProblematicDocumentFieldExtractionTest {

  private static final Logger logger = LoggerFactory.getLogger(ProblematicDocumentFieldExtractionTest.class);

  private String problematicDocument;

  /**
   * Helper method to extract fields using the JsonPropertyExtractor
   */
  private Map<String, Object> extractFields(String jsonDocument, List<String> fieldsToExtract) throws Exception {
    InputStream inputStream = new ByteArrayInputStream(jsonDocument.getBytes());
    String[] fieldsArray = fieldsToExtract.toArray(new String[0]);
    return JsonPropertyExtractor.extract(inputStream, fieldsArray);
  }

  @BeforeEach
  void setUp() {
    // The exact document content that causes the framework hang
    problematicDocument = "{\n" +
        "  \"affiliation\": \"Other\",\n" +
        "  \"attributeLastModified\": {\n" +
        "    \"favWorkspaces\": \"2023-02-03T17:18:39.3129958Z\",\n" +
        "    \"homePage\": \"2023-02-03T17:18:39.3129958Z\",\n" +
        "    \"recentEmailAddresses\": \"2022-12-14T23:48:43.0406871Z\",\n" +
        "    \"recentLocations\": \"2022-06-15T21:34:01.2119867Z\",\n" +
        "    \"recentWorkspaces\": \"2025-06-03T22:30:37.9493422Z\",\n" +
        "    \"recentlyAccessedDocs\": \"2024-10-16T17:45:05.5954426Z\",\n" +
        "    \"recentlyAddedDocs\": \"2024-10-16T17:45:05.5641711Z\",\n" +
        "    \"recentlyOpenedDocs\": \"2023-04-12T17:44:20.0466871Z\",\n" +
        "    \"searchRecent\": \"2025-06-03T21:12:20.7218805Z\"\n" +
        "  },\n" +
        "  \"cabinets\": [\n" +
        "    \"NG-L72JEAET\",\n" +
        "    \"NG-CF1JL14S\",\n" +
        "    \"NG-AJ2A72JV\",\n" +
        "    \"NG-7PVPQ903\",\n" +
        "    \"NG-L1T2FJVN\",\n" +
        "    \"NG-C0Y4LGR7\"\n" +
        "  ],\n" +
        "  \"defaultProfileTemplate\": \"\",\n" +
        "  \"email\": \"Haven.White@netdocuments.com\",\n" +
        "  \"emailChangeDate\": \"2022-05-03T20:33:02.6670542Z\",\n" +
        "  \"fullName\": \"Haven White\",\n" +
        "  \"givenName\": \"Haven\",\n" +
        "  \"guid\": \"DUCOT-RMXI2JXY\",\n" +
        "  \"highlightPeriod\": 7,\n" +
        "  \"inbox\": \"/Ducot5/7/t/8/5/^F220503163302354.nev\",\n" +
        "  \"initialLogin\": false,\n" +
        "  \"lastCabinet\": \"NG-AJ2A72JV\",\n" +
        "  \"lastLogin\": \"2025-06-03 19:26:43Z\",\n" +
        "  \"loginObject\": \"Login Account - DUCOT-RMXI2JXY| IP address - 216.190.237.4| Time - 20250424200212000| AppId - neWeb2,Login Account - DUCOT-RMXI2JXY| IP address - 127.0.0.1| Time - 20250428135719000| AppId - neWeb2\",\n"
        +
        "  \"marketingNotify\": true,\n" +
        "  \"nisa\": false,\n" +
        "  \"options\": 32960,\n" +
        "  \"personalContacts\": [\n" +
        "    \"DUCOT-RMXI2JXY\"\n" +
        "  ],\n" +
        "  \"personalStorage\": 0,\n" +
        "  \"pinStateAdminConsole\": true,\n" +
        "  \"primaryCabinet\": \"NG-AJ2A72JV\",\n" +
        "  \"pwHash\": \"$2a$11$00R5sWkLFgfdYdK.wIZNH.9jduiFFpu3lVRP2WVG0HoMLtLTCJ6Vq\",\n" +
        "  \"recentEmailAddresses\": \"[\\\"haven.white@netdocuments.com\\\"]\",\n" +
        "  \"regDate\": \"2022-05-03 20:33:02Z\",\n" +
        "  \"repositories\": [\n" +
        "    \"CA-7CT549PA\",\n" +
        "    \"CA-7NBB8TFM\"\n" +
        "  ],\n" +
        "  \"service\": \"DUCOT\",\n" +
        "  \"shortGuid\": \"RMXI2JXY\",\n" +
        "  \"surname\": \"White\",\n" +
        "  \"timeOffset\": 300,\n" +
        "  \"type\": \"user\",\n" +
        "  \"userLevel\": 5,\n" +
        "  \"username\": \"Haven.White@netdocuments.com\",\n" +
        "  \"intruderLockoutTime\": \"2025-03-14 17:00:17Z\",\n" +
        "  \"failedPasswordAttempts\": 2,\n" +
        "  \"groups\": [\n" +
        "    \"UG-F4GBRIYA\"\n" +
        "  ],\n" +
        "  \"authnUserId\": \"bbd210f7-dd74-4192-ad9f-ce06272c24a1\",\n" +
        "  \"storageCount\": 0\n" +
        "}";
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testBasicFieldExtraction() throws Exception {
    logger.info("Testing basic field extraction from problematic document DUCOT-RMXI2JXY");

    List<String> fieldsToExtract = Arrays.asList("guid", "email", "fullName", "type", "service");

    long startTime = System.currentTimeMillis();
    Map<String, Object> result = extractFields(problematicDocument, fieldsToExtract);
    long endTime = System.currentTimeMillis();

    logger.info("Basic field extraction completed in {}ms", endTime - startTime);

    assertNotNull(result);
    assertEquals("DUCOT-RMXI2JXY", result.get("guid"));
    assertEquals("Haven.White@netdocuments.com", result.get("email"));
    assertEquals("Haven White", result.get("fullName"));
    assertEquals("user", result.get("type"));
    assertEquals("DUCOT", result.get("service"));

    logger.info("Basic field extraction test passed - extracted {} fields", result.size());
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testLargeFieldExtraction() throws Exception {
    logger.info("Testing large field extraction including problematic searchRecent field");

    List<String> fieldsToExtract = Arrays.asList(
        "guid", "email", "fullName", "type", "service",
        "searchRecent", "recentlyAccessedDocs", "recentlyAddedDocs",
        "loginObject", "uiSettings");

    long startTime = System.currentTimeMillis();
    Map<String, Object> result = extractFields(problematicDocument, fieldsToExtract);
    long endTime = System.currentTimeMillis();

    logger.info("Large field extraction completed in {}ms", endTime - startTime);

    assertNotNull(result);
    assertEquals("DUCOT-RMXI2JXY", result.get("guid"));
    assertEquals("Haven.White@netdocuments.com", result.get("email"));

    // Log all extracted fields to see what's available
    logger.info("Extracted fields: {}", result.keySet());

    // Test the loginObject field (which should be present)
    Object loginObject = result.get("loginObject");
    assertNotNull(loginObject, "loginObject should be extracted");
    assertTrue(loginObject.toString().contains("Login Account"), "loginObject should contain login data");

    logger.info("Large field extraction test passed - extracted {} fields", result.size());
    logger.info("loginObject field length: {} characters", loginObject.toString().length());

    // Note: searchRecent and other large fields were simplified in this test
    // document
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testDirectoryWorkerFieldExtraction() throws Exception {
    logger.info("Testing exact field extraction used by directory worker");

    // These are the exact fields extracted by the directory worker
    List<String> fieldsToExtract = Arrays.asList(
        "guid", "email", "fullName", "givenName", "surname",
        "type", "service", "regDate", "lastLogin", "userLevel", "username");

    long startTime = System.currentTimeMillis();
    Map<String, Object> result = extractFields(problematicDocument, fieldsToExtract);
    long endTime = System.currentTimeMillis();

    logger.info("Directory worker field extraction completed in {}ms", endTime - startTime);

    assertNotNull(result);
    assertEquals(11, result.size(), "Should extract exactly 11 fields");

    // Verify all expected fields are present
    assertEquals("DUCOT-RMXI2JXY", result.get("guid"));
    assertEquals("Haven.White@netdocuments.com", result.get("email"));
    assertEquals("Haven White", result.get("fullName"));
    assertEquals("Haven", result.get("givenName"));
    assertEquals("White", result.get("surname"));
    assertEquals("user", result.get("type"));
    assertEquals("DUCOT", result.get("service"));
    assertEquals("2022-05-03 20:33:02Z", result.get("regDate"));
    assertEquals("2025-06-03 19:26:43Z", result.get("lastLogin"));
    assertEquals(5, result.get("userLevel"));
    assertEquals("Haven.White@netdocuments.com", result.get("username"));

    logger.info("Directory worker field extraction test passed - all 11 fields extracted correctly");
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testRepeatedExtractions() throws Exception {
    logger.info("Testing repeated extractions to check for memory leaks or performance degradation");

    List<String> fieldsToExtract = Arrays.asList(
        "guid", "email", "fullName", "type", "service", "loginObject");

    long totalTime = 0;
    int iterations = 10;

    for (int i = 0; i < iterations; i++) {
      long startTime = System.currentTimeMillis();
      Map<String, Object> result = extractFields(problematicDocument, fieldsToExtract);
      long endTime = System.currentTimeMillis();

      totalTime += (endTime - startTime);

      assertNotNull(result);
      assertEquals("DUCOT-RMXI2JXY", result.get("guid"));

      if (i % 3 == 0) {
        logger.info("Iteration {}: {}ms", i + 1, endTime - startTime);
      }
    }

    double averageTime = totalTime / (double) iterations;
    logger.info("Repeated extractions test passed - {} iterations, average time: {:.2f}ms",
        iterations, averageTime);

    // Performance assertion - should be fast with optimizations
    assertTrue(averageTime < 100,
        String.format("Average extraction time should be < 100ms, was %.2fms", averageTime));
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testDocumentSizeImpact() throws Exception {
    logger.info("Testing document size impact on extraction performance");

    List<String> fieldsToExtract = Arrays.asList("guid", "email", "type");

    // Test with full document
    long startTime = System.currentTimeMillis();
    Map<String, Object> result = extractFields(problematicDocument, fieldsToExtract);
    long endTime = System.currentTimeMillis();

    logger.info("Full document extraction: {}ms (size: {} chars)",
        endTime - startTime, problematicDocument.length());

    assertNotNull(result);
    assertEquals("DUCOT-RMXI2JXY", result.get("guid"));

    logger.info("Document size impact test passed - document size: {} characters",
        problematicDocument.length());
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testWithActualSearchRecentField() throws Exception {
    logger.info("Testing with actual searchRecent field content that might be problematic");

    // Create a document with the actual problematic searchRecent field
    String documentWithSearchRecent = "{\n" +
        "  \"guid\": \"DUCOT-RMXI2JXY\",\n" +
        "  \"email\": \"Haven.White@netdocuments.com\",\n" +
        "  \"type\": \"user\",\n" +
        "  \"searchRecent\": \"[{\\\"SearchId\\\":\\\"20250603091214\\\",\\\"UserCriteria\\\":\\\" =1001({0001})  =1002({0001})\\\",\\\"ParsedUserCriteria\\\":[{\\\"Field\\\":1001,\\\"Query\\\":\\\"{0001}\\\",\\\"Not\\\":false,\\\"DisplayValue\\\":null},{\\\"Field\\\":1002,\\\"Query\\\":\\\"{0001}\\\",\\\"Not\\\":false,\\\"DisplayValue\\\":null}],\\\"ParsedCriteria\\\":[{\\\"Field\\\":10,\\\"Query\\\":\\\"[NG-AJ2A72JV]\\\",\\\"Not\\\":false,\\\"DisplayValue\\\":null},{\\\"Field\\\":1001,\\\"Query\\\":\\\"{0001}\\\",\\\"Not\\\":false,\\\"DisplayValue\\\":null},{\\\"Field\\\":1002,\\\"Query\\\":\\\"{0001}\\\",\\\"Not\\\":false,\\\"DisplayValue\\\":null}],\\\"Refinements\\\":[],\\\"Cabinets\\\":[\\\"NG-AJ2A72JV\\\"],\\\"Name\\\":\\\"{0001} {0001}\\\",\\\"Repositories\\\":[\\\"CA-7NBB8TFM\\\"],\\\"SortBy\\\":[{\\\"Field\\\":7,\\\"Direction\\\":\\\"Descending\\\",\\\"UseMvpSorting\\\":false}],\\\"MetaData\\\":{\\\"listFlagsLayout\\\":0,\\\"allCabs\\\":false,\\\"crossRepository\\\":false,\\\"listScroll\\\":true},\\\"SearchSubFolders\\\":false,\\\"UserAndContainerCriteria\\\":[{\\\"Field\\\":10,\\\"Query\\\":\\\"[NG-AJ2A72JV]\\\",\\\"Not\\\":false,\\\"DisplayValue\\\":null},{\\\"Field\\\":1001,\\\"Query\\\":\\\"{0001}\\\",\\\"Not\\\":false,\\\"DisplayValue\\\":null},{\\\"Field\\\":1002,\\\"Query\\\":\\\"{0001}\\\",\\\"Not\\\":false,\\\"DisplayValue\\\":null}]}]\"\n"
        +
        "}";

    List<String> fieldsToExtract = Arrays.asList("guid", "email", "type", "searchRecent");

    long startTime = System.currentTimeMillis();
    Map<String, Object> result = extractFields(documentWithSearchRecent, fieldsToExtract);
    long endTime = System.currentTimeMillis();

    logger.info("SearchRecent field extraction completed in {}ms", endTime - startTime);

    assertNotNull(result);
    assertEquals("DUCOT-RMXI2JXY", result.get("guid"));

    Object searchRecent = result.get("searchRecent");
    assertNotNull(searchRecent, "searchRecent field should be extracted");
    assertTrue(searchRecent.toString().contains("SearchId"), "searchRecent should contain search data");

    logger.info("SearchRecent field extraction test passed - field length: {} characters",
        searchRecent.toString().length());
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testStringContainingJsonIsNotParsed() throws Exception {
    logger.info("Testing that JSON strings are treated as strings, not parsed as JSON");

    // Create a document where a string field contains JSON content
    String documentWithJsonString = "{\n" +
        "  \"guid\": \"TEST-123\",\n" +
        "  \"jsonString\": \"{\\\"nested\\\":{\\\"deep\\\":{\\\"value\\\":123}}}\",\n" +
        "  \"normalField\": \"simple string\"\n" +
        "}";

    List<String> fieldsToExtract = Arrays.asList("guid", "jsonString", "normalField");

    long startTime = System.currentTimeMillis();
    Map<String, Object> result = extractFields(documentWithJsonString, fieldsToExtract);
    long endTime = System.currentTimeMillis();

    logger.info("JSON string extraction completed in {}ms", endTime - startTime);

    assertNotNull(result);
    assertEquals("TEST-123", result.get("guid"));
    assertEquals("simple string", result.get("normalField"));

    // The jsonString should be returned as a string, not parsed as JSON
    Object jsonString = result.get("jsonString");
    assertNotNull(jsonString, "jsonString field should be extracted");
    assertTrue(jsonString instanceof String, "jsonString should be a String, not parsed JSON");
    assertEquals("{\"nested\":{\"deep\":{\"value\":123}}}", jsonString.toString());

    logger.info("JSON string test passed - string containing JSON was correctly treated as string");
    logger.info("Extracted jsonString: {}", jsonString);
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void testComplexJsonStringPerformance() throws Exception {
    logger.info("Testing performance with very complex JSON string content");

    // Create a document with a very complex JSON string (like searchRecent)
    String complexJsonString = "[{\"SearchId\":\"20250603091214\",\"UserCriteria\":\" =1001({0001})  =1002({0001})\",\"ParsedUserCriteria\":[{\"Field\":1001,\"Query\":\"{0001}\",\"Not\":false,\"DisplayValue\":null},{\"Field\":1002,\"Query\":\"{0001}\",\"Not\":false,\"DisplayValue\":null}],\"ParsedCriteria\":[{\"Field\":10,\"Query\":\"[NG-AJ2A72JV]\",\"Not\":false,\"DisplayValue\":null},{\"Field\":1001,\"Query\":\"{0001}\",\"Not\":false,\"DisplayValue\":null},{\"Field\":1002,\"Query\":\"{0001}\",\"Not\":false,\"DisplayValue\":null}],\"Refinements\":[],\"Cabinets\":[\"NG-AJ2A72JV\"],\"Name\":\"{0001} {0001}\",\"Repositories\":[\"CA-7NBB8TFM\"],\"SortBy\":[{\"Field\":7,\"Direction\":\"Descending\",\"UseMvpSorting\":false}],\"MetaData\":{\"listFlagsLayout\":0,\"allCabs\":false,\"crossRepository\":false,\"listScroll\":true},\"SearchSubFolders\":false,\"UserAndContainerCriteria\":[{\"Field\":10,\"Query\":\"[NG-AJ2A72JV]\",\"Not\":false,\"DisplayValue\":null},{\"Field\":1001,\"Query\":\"{0001}\",\"Not\":false,\"DisplayValue\":null},{\"Field\":1002,\"Query\":\"{0001}\",\"Not\":false,\"DisplayValue\":null}]},{\"SearchId\":\"20250603091116\",\"UserCriteria\":\"=3(The) =10([NG-AJ2A72JV])\",\"ParsedUserCriteria\":[{\"Field\":3,\"Query\":\"The\",\"Not\":false,\"DisplayValue\":null},{\"Field\":10,\"Query\":\"[NG-AJ2A72JV]\",\"Not\":false,\"DisplayValue\":null}],\"ParsedCriteria\":[{\"Field\":10,\"Query\":\"[NG-AJ2A72JV]\",\"Not\":false,\"DisplayValue\":null},{\"Field\":3,\"Query\":\"The\",\"Not\":false,\"DisplayValue\":null}],\"Refinements\":[],\"Cabinets\":[\"NG-AJ2A72JV\"],\"Name\":\"The\",\"Repositories\":[\"CA-7NBB8TFM\"],\"SortBy\":[{\"Field\":7,\"Direction\":\"Descending\",\"UseMvpSorting\":false}],\"MetaData\":{\"listFlagsLayout\":0,\"allCabs\":false,\"crossRepository\":false,\"listScroll\":true},\"SearchSubFolders\":false,\"UserAndContainerCriteria\":[{\"Field\":10,\"Query\":\"[NG-AJ2A72JV]\",\"Not\":false,\"DisplayValue\":null},{\"Field\":3,\"Query\":\"The\",\"Not\":false,\"DisplayValue\":null}]}]";

    String documentWithComplexJsonString = "{\n" +
        "  \"guid\": \"DUCOT-RMXI2JXY\",\n" +
        "  \"email\": \"Haven.White@netdocuments.com\",\n" +
        "  \"searchRecent\": \"" + complexJsonString.replace("\"", "\\\"") + "\",\n" +
        "  \"type\": \"user\"\n" +
        "}";

    List<String> fieldsToExtract = Arrays.asList("guid", "email", "searchRecent", "type");

    // Test multiple iterations to see if there's a performance issue
    long totalTime = 0;
    int iterations = 5;

    for (int i = 0; i < iterations; i++) {
      long startTime = System.currentTimeMillis();
      Map<String, Object> result = extractFields(documentWithComplexJsonString, fieldsToExtract);
      long endTime = System.currentTimeMillis();

      totalTime += (endTime - startTime);

      assertNotNull(result);
      assertEquals("DUCOT-RMXI2JXY", result.get("guid"));

      Object searchRecent = result.get("searchRecent");
      assertNotNull(searchRecent);
      assertTrue(searchRecent instanceof String, "searchRecent should remain a string");

      logger.info("Iteration {}: {}ms", i + 1, endTime - startTime);
    }

    double averageTime = totalTime / (double) iterations;
    logger.info("Complex JSON string test passed - {} iterations, average time: {:.2f}ms",
        iterations, averageTime);

    // Should still be very fast even with complex JSON strings
    assertTrue(averageTime < 50,
        String.format("Average time should be < 50ms for JSON strings, was %.2fms", averageTime));
  }
}
