package com.netdocuments.connect.kafka.handler.source;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.handler.source.SourceRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import org.mockito.Mockito;

@ExtendWith(MockitoExtension.class)
class NDSourceHandlerTest {

    private NDSourceHandler handler;
    private ObjectMapper objectMapper;

    @Mock
    private S3Client mockS3Client;

    @Mock
    private DocumentEvent mockEvent;

    @Mock
    private DocumentEvent mockDocEvent;

    @Mock
    private SourceHandlerParams mockParams;

    @Mock
    private SourceRecordBuilder mockBuilder;

    @BeforeEach
    void setUp() {
        handler = new NDSourceHandler();
        mockEvent = mock(DocumentEvent.class);
        mockParams = mock(SourceHandlerParams.class);
        mockBuilder = mock(SourceRecordBuilder.class);
        mockS3Client = mock(S3Client.class);
        objectMapper = new ObjectMapper();

        // Set up common mocks
        lenient().when(mockParams.documentEvent()).thenReturn(mockEvent);
        lenient().when(mockParams.topic()).thenReturn("test-topic");
    }

    private void initializeHandler(boolean enableS3, String fields, long threshold, String eventType, String suffix,
            String filterField, String filterValues, boolean filterAllowNull) {
        Map<String, String> props = new HashMap<>();
        props.put("couchbase.custom.handler.nd.fields", fields);
        props.put("couchbase.custom.handler.nd.cloudevent.type", eventType);

        if (enableS3) {
            props.put("couchbase.custom.handler.nd.s3.bucket", "test-bucket");
            props.put("couchbase.custom.handler.nd.s3.region", "us-west-2");
            props.put("couchbase.custom.handler.nd.s3.threshold", String.valueOf(threshold));
            props.put("couchbase.custom.handler.nd.s3.suffix", suffix);
        }

        if (filterField != null) {
            props.put("couchbase.custom.handler.nd.filter.field", filterField);
        }
        if (filterValues != null) {
            props.put("couchbase.custom.handler.nd.filter.values", filterValues);
        }
        props.put("couchbase.custom.handler.nd.filter.allow.null", String.valueOf(filterAllowNull));

        handler.init(props);
    }

    // Add an overloaded version of initializeHandler for backward compatibility
    private void initializeHandler(boolean enableS3, String fields, long threshold, String eventType, String suffix,
            String filterField, String filterValues) {
        initializeHandler(enableS3, fields, threshold, eventType, suffix, filterField, filterValues, false);
    }

    // Add another overload for the simplest case
    private void initializeHandler(boolean enableS3, String fields, long threshold, String eventType, String suffix) {
        initializeHandler(enableS3, fields, threshold, eventType, suffix, null, null, false);
    }

    @Test
    void testHandleMutationBelowS3Threshold() throws Exception {
        initializeHandler(true, "field1,field2,type", 100000, "test.event", ".json");
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals("MDucot5/qbti~240924115010829", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertEquals("value1", data.get("field1").asText());
        assertEquals("value2", data.get("field2").asText());
        assertEquals("type1", data.get("type").asText());

        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testHandleMutationAboveS3Threshold() throws Exception {
        initializeHandler(true, "*", 10, "test.event", ".json");
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");
        when(mockEvent.bucket()).thenReturn("test-bucket");
        when(mockEvent.revisionSeqno()).thenReturn(1L);

        handler.setS3Client(mockS3Client);

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals("MDucot5/qbti~240924115010829", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertNotNull(data.get("s3Bucket"));
        assertNotNull(data.get("s3Key"));

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());

        PutObjectRequest putObjectRequest = requestCaptor.getValue();
        assertEquals("test-bucket", putObjectRequest.bucket());
        assertTrue(putObjectRequest.key().startsWith("MDucot5/q/b/t/i/~240924115010829/"));
        assertTrue(putObjectRequest.key().endsWith(".json"));
    }

    @Test
    void testCloudEventHeaders() {
        initializeHandler(false, "field1,field2,type", 100000, "test.event", ".json");
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829",
                "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}", "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertCloudEventHeaders(result.headers());
    }

    @Test
    void testHandleExpiration() throws Exception {
        initializeHandler(false, "field1,field2,type", 100000, "test.event", ".s3");
        setupMockEvent(DocumentEvent.Type.EXPIRATION, "MDucot5/qbti~240924115010829", null, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals(Schema.STRING_SCHEMA, result.keySchema());
        assertEquals("MDucot5/qbti~240924115010829", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertEquals("expiration", data.get("event").asText());
        assertEquals("MDucot5/qbti~240924115010829", data.get("key").asText());

        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testS3Upload() {
        String content = "{\"documents\":{\"1\": {\"docProps\":{\"id\":\"doc123\"}}}}";
        initializeHandler(true, "*", 10, "test.event", ".s3");
        handler.setS3Client(mockS3Client);
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829",
                content, "test-bucket");
        when(mockEvent.bucket()).thenReturn("test-bucket");
        when(mockEvent.revisionSeqno()).thenReturn(123L); // Make sure revision number is set

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        handler.handle(params);

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());

        PutObjectRequest putObjectRequest = requestCaptor.getValue();
        assertEquals("test-bucket", putObjectRequest.bucket());
        assertTrue(putObjectRequest.key().startsWith("MDucot5/q/b/t/i/~240924115010829/"));
        assertTrue(putObjectRequest.key().contains("doc123"));
        assertTrue(putObjectRequest.key().endsWith("123.json")); // Should end with revision number
    }

    @Test
    void testDocPropsIdExtraction() throws Exception {
        initializeHandler(true, "*", 100000, "test.event", ".json");
        String content = "{\"documents\":{\"1\":{\"docProps\":{\"id\":\"doc123\"}}}}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        handler.handle(params); // This will trigger field extraction

        String s3Key = handler.generateS3Key(mockEvent);
        assertTrue(s3Key.contains("doc123"), "S3 key should contain the extracted docProps id");
    }

    @Test
    void testDocPropsIdExtractionWithInvalidDocument() throws Exception {
        initializeHandler(true, "*", 100000, "test.event", ".json");
        String content = "{\"documents\":{\"1\":{\"wrongField\":\"value\"}}}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "test-key", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        handler.handle(params); // This will trigger field extraction

        String s3Key = handler.generateS3Key(mockEvent);
        assertTrue(s3Key.startsWith("directory/"), "S3 key should use directory format for invalid documents");
    }

    @Test
    void testModifyKey() {
        String originalKey = "MDucot5/qbti~240924115010829";
        String modifiedKey = handler.modifyKey(originalKey);
        assertEquals("MDucot5/q/b/t/i/~240924115010829", modifiedKey);
    }

    @Test
    void testModifyKeyWithShortKey() {
        String originalKey = "short";
        String modifiedKey = handler.modifyKey(originalKey);
        assertEquals("short", modifiedKey);
    }

    private void setupMockEvent(DocumentEvent.Type type, String key, String content, String bucket) {
        if (content == null)
            content = "";
        lenient().when(mockEvent.type()).thenReturn(type);
        lenient().when(mockEvent.key()).thenReturn(key);
        lenient().when(mockEvent.content()).thenReturn(content.getBytes());
        lenient().when(mockEvent.bucket()).thenReturn(bucket);
        lenient().when(mockEvent.revisionSeqno()).thenReturn(123L);
    }

    private void assertCloudEventHeaders(Iterable<Header> headers) {
        boolean foundSpecVersion = false;
        boolean foundContentType = false;

        for (Header header : headers) {
            switch (header.key()) {
                case "ce_specversion":
                    assertEquals("1.0", header.value());
                    foundSpecVersion = true;
                    break;
                case "content-type":
                    assertEquals("application/cloudevents", header.value());
                    foundContentType = true;
                    break;
            }
        }

        assertTrue(foundSpecVersion, "CloudEvent spec version header not found");
        assertTrue(foundContentType, "CloudEvent content-type header not found");
    }

    @Test
    void testGenerateS3KeyForDirectory() {
        // Arrange
        DocumentEvent mockEvent = Mockito.mock(DocumentEvent.class);
        Mockito.when(mockEvent.key()).thenReturn("MDucot5/qbti~240924115010829");
        Mockito.when(mockEvent.revisionSeqno()).thenReturn(123L);

        // Act
        String result = handler.generateS3KeyForDirectory(mockEvent);

        // Assert
        assertEquals("directory/MDucot5/qbti~240924115010829/123.json", result);
    }

    @Test
    void testHandleSpecificFieldsExtractionMutation() {
        // Initialize handler with specific fields to extract
        initializeHandler(true, "field1,field2", 1000, "test.event", ".s3");

        // Prepare test data with matching fields
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\"}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        // Initialize extractedFields by calling extractFields
        Map<String, Object> fields = handler.extractFields(content.getBytes());
        assertNotNull(fields, "Extracted fields should not be null");
        assertFalse(fields.isEmpty(), "Extracted fields should not be empty");

        handler.setS3Client(mockS3Client);

        // Act
        boolean result = handler.handleSpecificFieldsExtractionMutation(mockEvent, DocumentEvent.Type.MUTATION,
                mockParams, mockBuilder);

        // Assert
        assertTrue(result, "Handler should successfully process the mutation");
        verify(mockBuilder).value(eq(null), any(byte[].class));
        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testHandleSpecificFieldsExtractionMutationWithS3Upload() {
        // Initialize handler
        initializeHandler(true, "field1,field2", 100, "test.event", ".s3");

        // Prepare test data
        String largeContent = "{\"field1\":\"" + "a".repeat(200) + "\",\"field2\":\"value2\"}";
        when(mockDocEvent.type()).thenReturn(DocumentEvent.Type.MUTATION);
        when(mockDocEvent.key()).thenReturn("MDucot5/qbti~240924115010829");
        when(mockDocEvent.bucket()).thenReturn("testBucket");
        when(mockDocEvent.revisionSeqno()).thenReturn(123L);

        // Need to extract fields first
        handler.extractFields(largeContent.getBytes());

        handler.setS3Client(mockS3Client);

        // Act
        boolean result = handler.handleSpecificFieldsExtractionMutation(mockDocEvent, DocumentEvent.Type.MUTATION,
                mockParams, mockBuilder);

        // Assert
        assertTrue(result);
        verify(mockBuilder).value(eq(null), any(byte[].class));
        verify(mockS3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testHandleSpecificFieldsExtractionMutationWithInvalidJson() {
        // Initialize handler
        initializeHandler(true, "field1,field2", 100, "test.event", ".s3");

        // Prepare test data
        byte[] invalidContent = "invalid json".getBytes();
        lenient().when(mockDocEvent.content()).thenReturn(invalidContent);

        // Need to extract fields first
        handler.extractFields(invalidContent);

        handler.setS3Client(mockS3Client);

        // Act
        boolean result = handler.handleSpecificFieldsExtractionMutation(mockDocEvent, DocumentEvent.Type.MUTATION,
                mockParams, mockBuilder);

        // Assert
        assertFalse(result);
        verify(mockBuilder, never()).value(any(), any());
        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testGenerateS3Key() {
        // Arrange
        initializeHandler(true, "*", 100000, "test.event", ".json");
        String content = "{\"documents\":{\"1\":{\"docProps\":{\"id\":\"doc123\"}}}}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        // Initialize extractedFields
        handler.extractFields(content.getBytes());

        // Act
        String result = handler.generateS3Key(mockEvent);

        // Assert
        String expectedKey = "MDucot5/q/b/t/i/~240924115010829/doc123/123.json";
        assertEquals(expectedKey, result);
    }

    @Test
    void testGenerateS3KeyForNonDocumentEvent() {
        // Arrange
        initializeHandler(true, "*", 100000, "test.event", ".json");
        String content = "{}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "shortKey", content, "test-bucket");

        // Initialize extractedFields
        handler.extractFields(content.getBytes());

        // Act
        String result = handler.generateS3Key(mockEvent);

        // Assert
        String expectedKey = "directory/shortKey/123.json";
        assertEquals(expectedKey, result);
    }

    @Test
    void testHandleMutationWithFieldFiltering() throws Exception {
        // Initialize with field filtering
        initializeHandler(true, "field1,field2", 100000, "test.event", ".json", "field1", "value1");
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals("MDucot5/qbti~240924115010829", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertEquals("value1", data.get("field1").asText());
        assertEquals("value2", data.get("field2").asText());
    }

    @Test
    void testHandleMutationWithFieldFilteringNoMatch() throws Exception {
        // Initialize with field filtering that won't match
        initializeHandler(true, "field1,field2", 100000, "test.event", ".json", "field1", "nonmatching");
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}";

        // Set up only the necessary mocks
        when(mockEvent.content()).thenReturn(content.getBytes());
        when(mockEvent.type()).thenReturn(DocumentEvent.Type.MUTATION);

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNull(result);
    }

    @Test
    void testHandleMutationWithNoFieldFiltering() throws Exception {
        // Initialize without field filtering
        initializeHandler(true, "field1,field2", 100000, "test.event", ".json", null, null);
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals("MDucot5/qbti~240924115010829", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertEquals("value1", data.get("field1").asText());
        assertEquals("value2", data.get("field2").asText());
    }

    @Test
    void testHandleMutationWithInvalidJson() throws Exception {
        initializeHandler(true, "field1,field2", 100000, "test.event", ".json");

        // Set up only the necessary mocks
        when(mockEvent.content()).thenReturn("invalid json".getBytes());
        when(mockEvent.type()).thenReturn(DocumentEvent.Type.MUTATION);

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNull(result);
    }

    @Test
    void testExtractFieldsWithMissingFields() throws Exception {
        // Test with no fields specified
        initializeHandler(true, null, 100000, "test.event", ".json");
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertNotNull(data.get("field1"));
        assertNotNull(data.get("field2"));
        assertNotNull(data.get("type"));
    }

    @Test
    void testExtractFieldsWithEmptyFields() throws Exception {
        // Test with empty fields
        initializeHandler(true, "", 100000, "test.event", ".json");
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertNotNull(data.get("field1"));
        assertNotNull(data.get("field2"));
        assertNotNull(data.get("type"));
    }

    @Test
    void testHandleMutationWithNullFieldValueAllowed() throws Exception {
        // Initialize with field filtering and allow null values
        initializeHandler(true, "field1,field2", 100000, "test.event", ".json", "field1", "value1", true);
        String content = "{\"field2\":\"value2\"}"; // field1 is missing/null
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals("MDucot5/qbti~240924115010829", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertNull(data.get("field1"));
        assertEquals("value2", data.get("field2").asText());
    }

    @Test
    void testHandleMutationWithNullFieldValueNotAllowed() throws Exception {
        // Initialize with field filtering and don't allow null values
        initializeHandler(true, "field1,field2", 100000, "test.event", ".json", "field1", "value1", false);
        String content = "{\"field2\":\"value2\"}"; // field1 is missing/null
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNull(result);
    }

    @Test
    void testHandleMutationWithExplicitNullFieldValueAllowed() throws Exception {
        // Initialize with field filtering and allow null values
        initializeHandler(true, "field1,field2", 100000, "test.event", ".json", "field1", "value1", true);
        String content = "{\"field1\": null, \"field2\":\"value2\"}"; // field1 is explicitly null
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result, "Result should not be null when null values are allowed");
        assertEquals("MDucot5/qbti~240924115010829", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertEquals("value2", data.get("field2").asText());
        // field1 should not be present in the output, as null is treated the same as
        // missing
        assertFalse(data.has("field1"), "field1 should not exist in the result when null");
    }

    @Test
    void testHandleMutationWithExplicitNullFieldValueNotAllowed() throws Exception {
        // Initialize with field filtering and don't allow null values
        initializeHandler(true, "field1,field2", 100000, "test.event", ".json", "field1", "value1", false);
        String content = "{\"field1\": null, \"field2\":\"value2\"}"; // field1 is explicitly null
        setupMockEvent(DocumentEvent.Type.MUTATION, "MDucot5/qbti~240924115010829", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNull(result);
    }
}
