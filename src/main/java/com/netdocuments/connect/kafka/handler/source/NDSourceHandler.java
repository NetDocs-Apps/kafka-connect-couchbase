/*
 * Copyright 2024 NetDocuments Software, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netdocuments.connect.kafka.handler.source;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.time.Duration;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.handler.source.SourceRecordBuilder;
import com.couchbase.connect.kafka.util.JsonPropertyExtractor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.util.*;

/**
 * NDSourceHandler extends RawJsonWithMetadataSourceHandler to provide custom
 * handling
 * for Couchbase documents, including field extraction, filtering, and S3 upload
 * capabilities.
 * It supports CloudEvents format and can be configured to filter documents
 * based on key patterns and document types.
 */
public class NDSourceHandler extends RawJsonWithMetadataSourceHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NDSourceHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Configuration keys
  private static final String FIELDS_CONFIG = "couchbase.custom.handler.nd.fields";
  private static final String S3_BUCKET_CONFIG = "couchbase.custom.handler.nd.s3.bucket";
  private static final String S3_REGION_CONFIG = "couchbase.custom.handler.nd.s3.region";
  private static final String AWS_PROFILE_CONFIG = "couchbase.custom.handler.nd.aws.profile";
  private static final String S3_THRESHOLD_CONFIG = "couchbase.custom.handler.nd.s3.threshold";
  private static final String CLOUD_EVENT_TYPE_CONFIG = "couchbase.custom.handler.nd.cloudevent.type";
  private static final String S3_SUFFIX_CONFIG = "couchbase.custom.handler.nd.s3.suffix";
  private static final String FILTER_FIELD_CONFIG = "couchbase.custom.handler.nd.filter.field";
  private static final String FILTER_VALUES_CONFIG = "couchbase.custom.handler.nd.filter.values";
  private static final String FILTER_ALLOW_NULL_CONFIG = "couchbase.custom.handler.nd.filter.allow.null";

  // Configuration definition
  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(FIELDS_CONFIG, ConfigDef.Type.LIST, "*", ConfigDef.Importance.HIGH,
          "The fields to extract from the document")
      .define(S3_BUCKET_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
          "The S3 bucket to upload documents to")
      .define(S3_REGION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
          "The AWS region for the S3 bucket")
      .define(AWS_PROFILE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
          "The AWS profile to use for S3 operations")
      .define(S3_THRESHOLD_CONFIG, ConfigDef.Type.LONG, 81920L, ConfigDef.Importance.MEDIUM,
          "The size threshold (in bytes) above which messages should be pushed to S3")
      .define(CLOUD_EVENT_TYPE_CONFIG, ConfigDef.Type.STRING, "com.netdocuments.ndserver.{bucket}.{type}",
          ConfigDef.Importance.MEDIUM,
          "The type of message that will be listed on cloud event")
      .define(S3_SUFFIX_CONFIG, ConfigDef.Type.STRING, ".S3", ConfigDef.Importance.LOW,
          "The suffix to append to S3 keys for uploaded messages")
      .define(FILTER_FIELD_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
          "JSON path to the field used for filtering (e.g., 'documents.1.docProps.type')")
      .define(FILTER_VALUES_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
          "Comma-separated list of values to filter on")
      .define(FILTER_ALLOW_NULL_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
          "If true, documents with null values for the filter field will pass the filter");

  private List<String> fields;
  private S3Client s3Client;
  private String s3Bucket;
  private boolean isS3Enabled;
  private String awsProfile;
  private long s3Threshold;
  private String cloudEventType;
  private String s3Suffix;
  private String filterField;
  private Set<String> filterValues;
  private boolean filterAllowNull;
  private static final String DOC_PROPS_ID_FIELD = "documents.1.docProps.id";

  // Timeout configuration for S3 and field extraction operations
  private static final Duration S3_API_CALL_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration S3_API_CALL_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  private static final int S3_UPLOAD_MAX_RETRIES = 3;
  private static final long FIELD_EXTRACTION_TIMEOUT_SECONDS = 10;

  // Executor for timeout-protected operations
  private final ExecutorService extractionExecutor = Executors.newCachedThreadPool();

  private Map<String, Object> extractedFields;

  private Set<String> getAllFieldsToExtract() {
    Set<String> allFields = new HashSet<>(fields);
    if (filterField != null && !filterField.isEmpty()) {
      allFields.add(filterField);
    }
    allFields.add(DOC_PROPS_ID_FIELD);
    return allFields;
  }

  /**
   * Extracts fields from document content with timeout protection.
   * If extraction takes longer than FIELD_EXTRACTION_TIMEOUT_SECONDS, it will be
   * cancelled and an empty map will be returned to prevent blocking the
   * connector.
   *
   * @param content The document content as bytes
   * @return Map of extracted field paths to values, or empty map on timeout/error
   */
  Map<String, Object> extractFields(byte[] content) {
    Set<String> allFields = getAllFieldsToExtract();
    Future<Map<String, Object>> future = extractionExecutor.submit(() -> {
      return JsonPropertyExtractor.extract(
          new ByteArrayInputStream(content),
          allFields.toArray(new String[0]));
    });

    try {
      extractedFields = future.get(FIELD_EXTRACTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      // Add debug logging
      LOGGER.info("Extracted fields: {}", extractedFields);
      if (filterField != null) {
        LOGGER.info("Filter field '{}' value: {}", filterField, extractedFields.get(filterField));
      }

      return extractedFields;
    } catch (TimeoutException e) {
      future.cancel(true);
      LOGGER.error("Field extraction timed out after {} seconds, returning empty map",
          FIELD_EXTRACTION_TIMEOUT_SECONDS);
      return new HashMap<>();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Field extraction was interrupted", e);
      return new HashMap<>();
    } catch (ExecutionException e) {
      LOGGER.error("Error while extracting fields from document", e.getCause());
      return new HashMap<>();
    }
  }

  private boolean passesValueFilter() {
    if (filterField == null || filterField.isEmpty() || filterValues == null || filterValues.isEmpty()) {
      return true;
    }
    Object fieldValue = extractedFields.get(filterField);
    String docId = extractDocPropsId();

    // Handle both missing fields and explicit null values
    if (fieldValue == null || (fieldValue instanceof JsonNode && ((JsonNode) fieldValue).isNull())) {
      LOGGER.info("Field value is null for {} (filterAllowNull={})", docId, filterAllowNull);
      return filterAllowNull;
    }

    LOGGER.info("Field value is '{}' for {} (type: {})",
        fieldValue.toString(),
        docId,
        fieldValue.getClass().getName());

    if (fieldValue instanceof String) {
      boolean contains = filterValues.contains(fieldValue);
      LOGGER.info("String value match: {}", contains);
      return contains;
    } else if (fieldValue instanceof List) {
      @SuppressWarnings("unchecked")
      List<String> values = (List<String>) fieldValue;
      if (values.size() == 1) {
        boolean contains = filterValues.contains(values.get(0));
        LOGGER.info("List value match: {}", contains);
        return contains;
      }
    } else {
      LOGGER.info("Unhandled field value type: {} ", fieldValue.getClass().getName());
    }
    LOGGER.info("Filtering out document");
    return false;
  }

  String extractDocPropsId() {
    Object value = extractedFields.get(DOC_PROPS_ID_FIELD);
    return value instanceof String ? (String) value : "unknown";
  }

  /**
   * Initializes the handler with the given configuration properties.
   * This method is called when the connector starts up.
   *
   * @param configProperties The configuration properties for the handler
   */
  @Override
  public void init(Map<String, String> configProperties) {
    super.init(configProperties);
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configProperties);

    initializeHandlerProperties(config);
    initializeS3Client(config);
    filterField = config.getString(FILTER_FIELD_CONFIG);
    String filterValuesStr = config.getString(FILTER_VALUES_CONFIG);
    filterValues = filterValuesStr != null && !filterValuesStr.isEmpty()
        ? new HashSet<>(Arrays.asList(filterValuesStr.split(",")))
        : null;
    filterAllowNull = config.getBoolean(FILTER_ALLOW_NULL_CONFIG);

    if (filterField != null && !filterField.isEmpty() && filterValues != null && !filterValues.isEmpty()) {
      LOGGER.info("Initialized value filtering with field '{}' and values: {}", filterField, filterValues);
    }
  }

  /**
   * Initializes the handler properties including fields, types, key pattern, and
   * CloudEvent settings.
   */
  private void initializeHandlerProperties(AbstractConfig config) {
    // Initialize fields with a default of "*" if not provided
    fields = config.getList(FIELDS_CONFIG);
    if (fields == null || fields.isEmpty()) {
      fields = Collections.singletonList("*");
    }

    // Initialize CloudEvent type
    cloudEventType = config.getString(CLOUD_EVENT_TYPE_CONFIG);
  }

  /**
   * Initializes the S3 client for document uploads with timeout protection.
   * Configures API call timeout and per-attempt timeout to prevent indefinite
   * blocking.
   */
  private void initializeS3Client(AbstractConfig config) {
    s3Bucket = config.getString(S3_BUCKET_CONFIG);
    String s3Region = config.getString(S3_REGION_CONFIG);
    // Initialize S3 threshold
    s3Threshold = config.getLong(S3_THRESHOLD_CONFIG);
    // Initialize S3 suffix
    s3Suffix = config.getString(S3_SUFFIX_CONFIG);
    if (s3Bucket == null || s3Region == null) {
      isS3Enabled = false;
    } else {
      // Initialize S3 client with timeout configuration
      isS3Enabled = true;
      awsProfile = config.getString(AWS_PROFILE_CONFIG);
      LOGGER.info(
          "Initializing S3 client with bucket={}, region={}, profile={}, apiCallTimeout={}s, attemptTimeout={}s",
          s3Bucket, s3Region, awsProfile,
          S3_API_CALL_TIMEOUT.getSeconds(), S3_API_CALL_ATTEMPT_TIMEOUT.getSeconds());

      // Configure timeouts to prevent indefinite blocking on S3 operations
      ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
          .apiCallTimeout(S3_API_CALL_TIMEOUT)
          .apiCallAttemptTimeout(S3_API_CALL_ATTEMPT_TIMEOUT)
          .build();

      if (awsProfile != null) {
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder()
            .profileName(awsProfile) // Your desired profile name
            .build();
        AwsCredentialsProviderChain credentialsProviderChain = AwsCredentialsProviderChain.builder()
            .addCredentialsProvider(credentialsProvider)
            .addCredentialsProvider(EnvironmentVariableCredentialsProvider.create())
            .build();
        s3Client = S3Client.builder()
            .region(Region.of(s3Region))
            .credentialsProvider(credentialsProviderChain)
            .overrideConfiguration(overrideConfig)
            .build();
      } else {
        s3Client = S3Client.builder()
            .region(Region.of(s3Region))
            .overrideConfiguration(overrideConfig)
            .build();
      }
    }
  }

  /**
   * Handles a document event and builds a SourceRecord.
   *
   * @param params The parameters containing the document event and other context
   * @return A SourceRecordBuilder with the processed event, or null if the event
   *         should be skipped
   */
  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    SourceRecordBuilder builder = new SourceRecordBuilder();

    addCloudEventHeaders(builder);

    if (!buildValue(params, builder)) {
      return null;
    }

    return builder
        .topic(getTopic(params))
        .key(Schema.STRING_SCHEMA, params.documentEvent().key());
  }

  /**
   * Adds CloudEvent-specific headers to the SourceRecordBuilder.
   */
  private void addCloudEventHeaders(SourceRecordBuilder builder) {
    builder.headers().addString("ce_specversion", "1.0");
    builder.headers().addString("content-type", "application/cloudevents");
  }

  /**
   * Builds the value for the SourceRecord based on the document event.
   *
   * @param params  The parameters containing the document event and other context
   * @param builder The SourceRecordBuilder to populate
   * @return true if the value was successfully built, false otherwise
   */
  @Override
  protected boolean buildValue(SourceHandlerParams params, SourceRecordBuilder builder) {
    final DocumentEvent docEvent = params.documentEvent();
    final DocumentEvent.Type type = docEvent.type();

    if (type == DocumentEvent.Type.EXPIRATION || type == DocumentEvent.Type.DELETION) {
      return handleDeletionOrExpiration(docEvent, type, builder);
    } else if (type == DocumentEvent.Type.MUTATION) {
      if (!isValidJson(docEvent.content())) {
        LOGGER.warn("Skipping non-JSON document: bucket={} key={}", docEvent.bucket(), docEvent.qualifiedKey());
        return false;
      }

      // Extract all fields once
      extractedFields = extractFields(docEvent.content());

      // Apply value filtering before proceeding
      if (!passesValueFilter()) {
        LOGGER.debug("Document filtered out based on field {} values", filterField);
        return false;
      }

      if (fields.size() == 1 && fields.get(0).equals("*")) {
        return handleFullDocumentMutation(docEvent, params, builder);
      }
      return handleSpecificFieldsExtractionMutation(docEvent, type, params, builder);
    } else {
      LOGGER.warn("unexpected event type {}", type);
      return false;
    }
  }

  /**
   * Handles deletion or expiration events.
   */
  private boolean handleDeletionOrExpiration(DocumentEvent docEvent, DocumentEvent.Type type,
      SourceRecordBuilder builder) {
    Map<String, Object> newValue = new HashMap<>();
    newValue.put("event", type.schemaName());
    newValue.put("key", docEvent.key());
    try {
      byte[] value = convertToBytes(newValue, docEvent, "");
      builder.value(null, value);
      return true;
    } catch (DataException e) {
      LOGGER.error("Failed to serialize data", e);
      return false;
    }
  }

  /**
   * Handles mutation events, including uploading to S3.
   */
  private boolean handleFullDocumentMutation(DocumentEvent docEvent, SourceHandlerParams params,
      SourceRecordBuilder builder) {
    if (params.noValue()) {
      builder.value(null, convertToBytes(null, docEvent, ""));
      return true;
    }

    byte[] document = docEvent.content();
    String typeSuffix = "";
    if (isS3Enabled && document.length > s3Threshold) {
      typeSuffix = s3Suffix;
      String s3Key = generateS3Key(docEvent);
      uploadToS3(s3Key, document);
      document = String.format("{\"s3Bucket\":\"%s\",\"s3Key\":\"%s\"}", s3Bucket, s3Key).getBytes();
    }
    builder.value(null, withCloudEvent(document, docEvent, typeSuffix));
    return true;
  }

  /**
   * Uploads the document content to S3 with retry logic.
   * Retries up to S3_UPLOAD_MAX_RETRIES times on failure before throwing.
   */
  private void uploadToS3(String s3Key, byte[] document) {
    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
        .bucket(s3Bucket)
        .key(s3Key)
        .contentType("application/json")
        .build();

    Exception lastException = null;
    for (int attempt = 1; attempt <= S3_UPLOAD_MAX_RETRIES; attempt++) {
      try {
        s3Client.putObject(putObjectRequest, RequestBody.fromBytes(document));
        LOGGER.debug("Uploaded document to S3: s3://{}/{}", s3Bucket, s3Key);
        return;
      } catch (Exception e) {
        lastException = e;
        if (attempt < S3_UPLOAD_MAX_RETRIES) {
          LOGGER.warn("Failed to upload document to S3 (attempt {}/{}): {}",
              attempt, S3_UPLOAD_MAX_RETRIES, e.getMessage());
        }
      }
    }
    LOGGER.error("Failed to upload document to S3 after {} attempts: {}",
        S3_UPLOAD_MAX_RETRIES, lastException.getMessage(), lastException);
    throw new RuntimeException("S3 upload failed after " + S3_UPLOAD_MAX_RETRIES + " attempts", lastException);
  }

  /**
   * Generates a unique S3 key for the document based on its key, document
   * property ID,and revision sequence number if it is a document type event.
   * If it is not a document type event it calls the generateS3KeyForDirectory
   * method and returns the result.
   *
   * @param docEvent The document event containing metadata about the document
   * @return A unique S3 key string
   */
  String generateS3Key(DocumentEvent docEvent) {
    String originalKey = docEvent.key();
    long revisionSeqno = docEvent.revisionSeqno();

    String docPropsId = extractDocPropsId();
    String modifiedKey = modifyKey(originalKey);

    if (modifiedKey == originalKey && (docPropsId == null || docPropsId.equals("unknown"))) {
      return generateS3KeyForDirectory(docEvent);
    }

    return String.format("%s/%s/%d.json", modifiedKey, docPropsId, revisionSeqno);
  }

  /**
   * Modifies the original key by adding '/' after each of the next four letters
   * after the existing '/'.
   *
   * @param originalKey The original document key
   * @return The modified key with additional '/' characters
   */
  String modifyKey(String originalKey) {
    int firstSlashIndex = originalKey.indexOf('/');
    if (firstSlashIndex == -1 || firstSlashIndex + 5 > originalKey.length()) {
      return originalKey;
    }

    StringBuilder modifiedKey = new StringBuilder(originalKey);
    for (int i = 1; i <= 4; i++) {
      modifiedKey.insert(firstSlashIndex + i * 2, '/');
    }
    return modifiedKey.toString();
  }

  /**
   * Handles extraction of specific fields from the document.
   */
  boolean handleSpecificFieldsExtractionMutation(DocumentEvent docEvent, DocumentEvent.Type type,
      SourceHandlerParams params, SourceRecordBuilder builder) {
    final Map<String, Object> newValue = createMutationValue(docEvent);
    if (newValue == null) {
      return false;
    }

    try {
      byte[] value = convertToBytes(newValue, docEvent, "");
      String typeSuffix = "";
      if (value.length > s3Threshold) {
        typeSuffix = s3Suffix;
        String s3Key = generateS3Key(docEvent);
        uploadToS3(s3Key, value);
        value = String.format("{\"s3Bucket\":\"%s\",\"s3Key\":\"%s\"}", s3Bucket, s3Key).getBytes();
        // Wrap the S3 reference in a cloud event with the appropriate suffix
        value = withCloudEvent(value, docEvent, typeSuffix);
      }
      builder.value(null, value);
      return true;
    } catch (DataException e) {
      LOGGER.error("Failed to serialize data", e);
      return false;
    }
  }

  /**
   * Generates an S3 key for directory-based storage of document events.
   *
   * @param docEvent The document event containing metadata about the document
   * @return A unique S3 key string for directory-based storage
   */
  String generateS3KeyForDirectory(DocumentEvent docEvent) {
    String key = docEvent.key();
    long revisionSeqno = docEvent.revisionSeqno();

    return String.format("directory/%s/%d.json", key, revisionSeqno);
  }

  /**
   * Creates a value map for mutation events, extracting specified fields.
   */
  private Map<String, Object> createMutationValue(DocumentEvent docEvent) {
    if (extractedFields == null || extractedFields.isEmpty()) {
      return null;
    }

    Map<String, Object> newValue = new HashMap<>();
    for (String field : fields) {
      Object value = extractedFields.get(field);
      if (value != null) {
        newValue.put(field, value);
      }
    }
    newValue.put("event", DocumentEvent.Type.MUTATION.schemaName());
    newValue.put("key", docEvent.key());
    return newValue;
  }

  /**
   * Converts a value to bytes, applying CloudEvent format if necessary.
   */
  private byte[] convertToBytes(Map<String, Object> value, DocumentEvent docEvent, String typeSuffix) {
    return withCloudEvent(serializeToJson(value), docEvent, typeSuffix);
  }

  /**
   * Serializes an object to JSON bytes.
   */
  private byte[] serializeToJson(Object value) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(value);
    } catch (IOException e) {
      throw new DataException("Failed to serialize data", e);
    }
  }

  /**
   * Wraps the given value in a CloudEvent format.
   */
  private byte[] withCloudEvent(byte[] value, DocumentEvent documentEvent, String typeSuffix) {
    Map<String, Object> cloudEventData = createCloudEventData(documentEvent, typeSuffix);
    byte[] cloudEventBytes = serializeToJson(cloudEventData);

    ByteArrayBuilder result = new ByteArrayBuilder(
        cloudEventBytes.length + ",\"data\":".getBytes().length + value.length)
        .append(cloudEventBytes, cloudEventBytes.length - 1)
        .append(",\"data\":".getBytes())
        .append(value)
        .append((byte) '}');
    return result.build();
  }

  /**
   * Creates the CloudEvent metadata for a document event.
   */
  private Map<String, Object> createCloudEventData(DocumentEvent documentEvent, String typeSuffix) {
    Map<String, Object> data = new HashMap<>();
    data.put("specversion", "1.0");
    data.put("id", documentEvent.key() + "-" + documentEvent.revisionSeqno());
    data.put("type", getCloudEventType(documentEvent, typeSuffix));
    data.put("source", "netdocs://ndserver/" + documentEvent.bucket());
    Instant ts = documentEvent.timestamp();
    data.put("time", (ts != null ? ts : Instant.now()).toString());
    data.put("datacontenttype", "application/json;charset=utf-8");
    data.put("partitionkey", documentEvent.key());
    data.put("traceparent", UUID.randomUUID().toString());
    return data;
  }

  /**
   * Gets the CloudEvent type for a document event.
   */
  private String getCloudEventType(DocumentEvent documentEvent, String suffix) {
    return String.format("%s%s", cloudEventType.replace("{bucket}", documentEvent.bucket())
        .replace("{type}", documentEvent.type().schemaName()), suffix);
  }

  // For testing purposes
  void setS3Client(S3Client s3Client) {
    this.s3Client = s3Client;
  }
}
