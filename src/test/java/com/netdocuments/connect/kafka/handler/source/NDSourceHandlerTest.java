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
import org.mockito.MockitoAnnotations;

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
        MockitoAnnotations.openMocks(this);
        handler = new NDSourceHandler();
        objectMapper = new ObjectMapper();
    }

    private void initializeHandler(boolean useS3, String fields, long s3Threshold, String cloudEventType,
            String s3Suffix) {
        Map<String, String> config = new HashMap<>();
        config.put("couchbase.custom.handler.nd.fields", fields);
        config.put("couchbase.custom.handler.nd.cloudevent.type", cloudEventType);
        if (useS3) {
            config.put("couchbase.custom.handler.nd.s3.bucket", "test-bucket");
            config.put("couchbase.custom.handler.nd.s3.region", "us-west-2");
            config.put("couchbase.custom.handler.nd.s3.threshold", String.valueOf(s3Threshold));
            config.put("couchbase.custom.handler.nd.s3.suffix", s3Suffix);
        }

        handler.init(config);
    }

    @Test
    void testHandleMutationBelowS3Threshold() throws Exception {
        initializeHandler(true, "field1,field2,type", 100000, "test.event", ".json");
        String content = "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}";
        setupMockEvent(DocumentEvent.Type.MUTATION, "test-key", content, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals("test-key", result.key());

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
        setupMockEvent(DocumentEvent.Type.MUTATION, "test-key", content, "test-bucket");
        when(mockEvent.bucket()).thenReturn("test-bucket");
        when(mockEvent.revisionSeqno()).thenReturn(1L);

        handler.setS3Client(mockS3Client);

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals("test-key", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertNotNull(data.get("s3Bucket"));
        assertNotNull(data.get("s3Key"));

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());

        PutObjectRequest putObjectRequest = requestCaptor.getValue();
        assertEquals("test-bucket", putObjectRequest.bucket());
        assertTrue(putObjectRequest.key().startsWith("directory/test-key/"));
        assertTrue(putObjectRequest.key().endsWith(".json"));
    }

    @Test
    void testCloudEventHeaders() {
        initializeHandler(false, "field1,field2,type", 100000, "test.event", ".json");
        setupMockEvent(DocumentEvent.Type.MUTATION, "test-key",
                "{\"field1\":\"value1\",\"field2\":\"value2\",\"type\":\"type1\"}", "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertCloudEventHeaders(result.headers());
    }

    @Test
    void testHandleExpiration() throws Exception {
        initializeHandler(false, "field1,field2,type", 100000, "test.event", ".s3");
        setupMockEvent(DocumentEvent.Type.EXPIRATION, "test-key", null, "test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        SourceRecordBuilder result = handler.handle(params);

        assertNotNull(result);
        assertEquals(Schema.STRING_SCHEMA, result.keySchema());
        assertEquals("test-key", result.key());

        JsonNode jsonNode = objectMapper.readTree((byte[]) result.value());
        JsonNode data = jsonNode.get("data");
        assertEquals("expiration", data.get("event").asText());
        assertEquals("test-key", data.get("key").asText());

        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testS3Upload() {
        String content = "{\"documents\":{\"1\": {\"docProps\":{\"id\":\"doc123\"}}}}";
        initializeHandler(true, "*", 10, "test.event", ".s3");
        handler.setS3Client(mockS3Client);
        setupMockEvent(DocumentEvent.Type.MUTATION, "test-key",
                content, "test-bucket");
        when(mockEvent.bucket()).thenReturn("test-bucket");

        SourceHandlerParams params = new SourceHandlerParams(mockEvent, "test-topic", false);
        handler.handle(params);

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());

        PutObjectRequest putObjectRequest = requestCaptor.getValue();
        assertEquals("test-bucket", putObjectRequest.bucket());
        assertTrue(putObjectRequest.key().startsWith("test-key/"));
        assertTrue(putObjectRequest.key().contains(("doc123")));
        assertTrue(putObjectRequest.key().endsWith("0.json"));
    }

    @Test
    void testExtractDocPropsId() {
        byte[] content = "{\"documents\":{\"1\": {\"docProps\":{\"id\":\"doc123\"}}}}".getBytes();
        String docPropsId = handler.extractDocPropsId(content);
        assertEquals("doc123", docPropsId);
    }

    @Test
    void testExtractDocPropsIdWithInvalidJson() {
        byte[] content = "invalid json".getBytes();
        String docPropsId = handler.extractDocPropsId(content);
        assertEquals("unknown", docPropsId);
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
        when(mockEvent.type()).thenReturn(type);
        when(mockEvent.key()).thenReturn(key);
        when(mockEvent.bucket()).thenReturn(bucket);
        if (content != null) {
            when(mockEvent.content()).thenReturn(content.getBytes());
        }
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
        Mockito.when(mockEvent.key()).thenReturn("testKey");
        Mockito.when(mockEvent.revisionSeqno()).thenReturn(123L);

        // Act
        String result = handler.generateS3KeyForDirectory(mockEvent);

        // Assert
        assertEquals("directory/testKey/123.json", result);
    }

    @Test
    void testHandleSpecificFieldsExtractionMutation() {
        initializeHandler(true, "field1,fields2", 1000, "test.event", ".s3");
        // Arrange
        when(mockDocEvent.type()).thenReturn(DocumentEvent.Type.MUTATION);
        when(mockDocEvent.content()).thenReturn("{\"field1\":\"value1\",\"field2\":\"value2\"}".getBytes());
        when(mockDocEvent.key()).thenReturn("testKey");
        when(mockDocEvent.bucket()).thenReturn("testBucket");
        when(mockDocEvent.revisionSeqno()).thenReturn(123L);
        // when(mockParams.documentEvent()).thenReturn(mockDocEvent);
        handler.setS3Client(mockS3Client);
        // Act
        boolean result = handler.handleSpecificFieldsExtractionMutation(mockDocEvent, DocumentEvent.Type.MUTATION,
                mockParams, mockBuilder);

        // Assert
        assertTrue(result);
        verify(mockBuilder).value(eq(null), any(byte[].class));
        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testHandleSpecificFieldsExtractionMutationWithS3Upload() {
        initializeHandler(true, "field1,fields2", 100, "test.event", ".s3");
        // Arrange
        String largeContent = "{\"field1\":\"" + "a".repeat(200) + "\",\"field2\":\"value2\"}";
        when(mockDocEvent.type()).thenReturn(DocumentEvent.Type.MUTATION);
        when(mockDocEvent.content()).thenReturn(largeContent.getBytes());
        when(mockDocEvent.key()).thenReturn("testKey");
        when(mockDocEvent.bucket()).thenReturn("testBucket");
        when(mockDocEvent.revisionSeqno()).thenReturn(123L);
        // when(mockParams.documentEvent()).thenReturn(mockDocEvent);
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
        initializeHandler(true, "field1,fields2", 100, "test.event", ".s3");
        // Arrange
        when(mockDocEvent.content()).thenReturn("invalid json".getBytes());
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
        String originalKey = "MDucot5/qbti~240924115010829";
        long revisionSeqno = 123L;
        byte[] content = "{\"documents\":{\"1\":{\"docProps\":{\"id\":\"doc123\"}}}}".getBytes();

        when(mockEvent.key()).thenReturn(originalKey);
        when(mockEvent.revisionSeqno()).thenReturn(revisionSeqno);
        when(mockEvent.content()).thenReturn(content);

        // Act
        String result = handler.generateS3Key(mockEvent);

        // Assert
        String expectedKey = "MDucot5/q/b/t/i/~240924115010829/doc123/123.json";
        assertEquals(expectedKey, result);
    }

    @Test
    void testGenerateS3KeyForNonDocumentEvent() {
        // Arrange
        String originalKey = "shortKey";
        long revisionSeqno = 456L;
        byte[] content = "{}".getBytes();

        when(mockEvent.key()).thenReturn(originalKey);
        when(mockEvent.revisionSeqno()).thenReturn(revisionSeqno);
        when(mockEvent.content()).thenReturn(content);

        // Act
        String result = handler.generateS3Key(mockEvent);

        // Assert
        String expectedKey = "directory/shortKey/456.json";
        assertEquals(expectedKey, result);
    }
}
