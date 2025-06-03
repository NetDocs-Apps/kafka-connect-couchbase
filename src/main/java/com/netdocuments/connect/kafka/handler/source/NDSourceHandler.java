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
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

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
  private Map<String, Object> extractedFields;

  // Executor service for timeout protection
  private ExecutorService fieldExtractionExecutor;

  private Set<String> getAllFieldsToExtract() {
    Set<String> allFields = new HashSet<>(fields);
    if (filterField != null && !filterField.isEmpty()) {
      allFields.add(filterField);
    }
    allFields.add(DOC_PROPS_ID_FIELD);
    return allFields;
  }

  /**
   * Performs lightweight filtering check by extracting only the filter field.
   * This avoids expensive full field extraction for documents that will be
   * filtered out.
   */
  private boolean passesLightweightFilter(byte[] content) {
    if (filterField == null || filterField.isEmpty() || filterValues == null || filterValues.isEmpty()) {
      return true; // No filtering configured
    }

    try {
      // Extract only the filter field for lightweight check
      Map<String, Object> filterFieldOnly = JsonPropertyExtractor.extract(
          new ByteArrayInputStream(content),
          new String[] { filterField });

      Object fieldValue = filterFieldOnly.get(filterField);

      // Handle null values
      if (fieldValue == null || (fieldValue instanceof JsonNode && ((JsonNode) fieldValue).isNull())) {
        LOGGER.debug("Filter field '{}' is null (filterAllowNull={})", filterField, filterAllowNull);
        return filterAllowNull;
      }

      // Check filter values
      if (fieldValue instanceof String) {
        boolean matches = filterValues.contains(fieldValue);
        LOGGER.debug("Filter field '{}' value '{}' matches: {}", filterField, fieldValue, matches);
        return matches;
      } else if (fieldValue instanceof List) {
        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) fieldValue;
        if (values.size() == 1) {
          boolean matches = filterValues.contains(values.get(0));
          LOGGER.debug("Filter field '{}' list value '{}' matches: {}", filterField, values.get(0), matches);
          return matches;
        }
      }

      LOGGER.debug("Filter field '{}' value type '{}' not supported, filtering out",
          filterField, fieldValue.getClass().getName());
      return false;

    } catch (Exception e) {
      LOGGER.warn("Error during lightweight filtering for field '{}', allowing document through", filterField, e);
      return true; // Allow through on error to avoid blocking
    }
  }

  /**
   * Extracts all configured fields with timeout protection.
   */
  Map<String, Object> extractFields(byte[] content) {
    try {
      Set<String> allFields = getAllFieldsToExtract();

      // Use timeout protection for field extraction
      Future<Map<String, Object>> future = fieldExtractionExecutor.submit(() -> {
        return JsonPropertyExtractor.extract(
            new ByteArrayInputStream(content),
            allFields.toArray(new String[0]));
      });

      extractedFields = future.get(3, TimeUnit.SECONDS); // 3-second timeout

      // Add debug logging
      LOGGER.debug("Extracted {} fields successfully", extractedFields.size());
      if (filterField != null && LOGGER.isDebugEnabled()) {
        LOGGER.debug("Filter field '{}' value: {}", filterField, extractedFields.get(filterField));
      }

      return extractedFields;

    } catch (TimeoutException e) {
      LOGGER.warn("Field extraction timed out after 3 seconds, using empty field map");
      return new HashMap<>();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Field extraction was interrupted, using empty field map");
      return new HashMap<>();
    } catch (ExecutionException e) {
      LOGGER.error("Error during field extraction", e.getCause());
      return new HashMap<>();
    } catch (Exception e) {
      LOGGER.error("Unexpected error while extracting fields from document", e);
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
    initializeFieldExtractionExecutor();

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
   * Initializes the executor service for field extraction timeout protection.
   */
  private void initializeFieldExtractionExecutor() {
    // Use a single-threaded executor with a meaningful thread name
    fieldExtractionExecutor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "field-extraction-timeout-" + Thread.currentThread().getName());
      t.setDaemon(true); // Don't prevent JVM shutdown
      return t;
    });
    LOGGER.info("Initialized field extraction executor with timeout protection");
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
   * Initializes the S3 client for document uploads.
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
      // Initialize S3 client
      isS3Enabled = true;
      awsProfile = config.getString(AWS_PROFILE_CONFIG);
      LOGGER.info("Initializing S3 client with bucket={}, region={}, profile={}", s3Bucket, s3Region, awsProfile);

      // Configure timeouts to prevent hanging during shutdown
      ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
          .apiCallTimeout(Duration.ofSeconds(30))
          .apiCallAttemptTimeout(Duration.ofSeconds(10))
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
            .overrideConfiguration(clientConfig)
            .build();
      } else {
        s3Client = S3Client.builder()
            .region(Region.of(s3Region))
            .overrideConfiguration(clientConfig)
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

      // OPTIMIZATION 1: Apply lightweight filtering BEFORE expensive field extraction
      if (!passesLightweightFilter(docEvent.content())) {
        LOGGER.debug("Document filtered out by lightweight filter on field '{}'", filterField);
        return false;
      }

      // OPTIMIZATION 2: Only extract fields for documents that pass filtering
      extractedFields = extractFields(docEvent.content());

      // Final validation using extracted fields (for cases where lightweight filter
      // passed but full extraction reveals issues)
      if (!passesValueFilter()) {
        LOGGER.debug("Document filtered out by full field validation on field '{}'", filterField);
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
   * Uploads the document content to S3.
   * Uses defensive error handling to prevent S3 failures from blocking connector
   * shutdown.
   */
  private void uploadToS3(String s3Key, byte[] document) {
    try {
      PutObjectRequest putObjectRequest = PutObjectRequest.builder()
          .bucket(s3Bucket)
          .key(s3Key)
          .contentType("application/json")
          .build();

      s3Client.putObject(putObjectRequest, RequestBody.fromBytes(document));
      LOGGER.debug("Uploaded document to S3: s3://{}/{}", s3Bucket, s3Key);
    } catch (Exception e) {
      LOGGER.error("Failed to upload document to S3: s3://{}/{}. Error: {}", s3Bucket, s3Key, e.getMessage(), e);
      // Don't re-throw the exception to prevent blocking connector shutdown
      // The document will be processed without S3 upload
    }
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
      if (value.length > s3Threshold) {
        String s3Key = generateS3Key(docEvent);
        uploadToS3(s3Key, value);
        value = String.format("{\"s3Bucket\":\"%s\",\"s3Key\":\"%s\"}", s3Bucket, s3Key).getBytes();
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
    data.put("time", Instant.now().toString());
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

  /**
   * Cleanup method to properly close resources during shutdown.
   * This method should be called when the connector is stopping.
   */
  public void cleanup() {
    // Shutdown field extraction executor
    if (fieldExtractionExecutor != null) {
      try {
        LOGGER.info("Shutting down field extraction executor during handler cleanup");
        fieldExtractionExecutor.shutdown();
        if (!fieldExtractionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          LOGGER.warn("Field extraction executor did not terminate gracefully, forcing shutdown");
          fieldExtractionExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while shutting down field extraction executor");
        fieldExtractionExecutor.shutdownNow();
      } catch (Exception e) {
        LOGGER.warn("Error shutting down field extraction executor during cleanup", e);
      } finally {
        fieldExtractionExecutor = null;
      }
    }

    // Shutdown S3 client
    if (s3Client != null) {
      try {
        LOGGER.info("Closing S3 client during handler cleanup");
        s3Client.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing S3 client during cleanup", e);
      } finally {
        s3Client = null;
      }
    }
  }

  // For testing purposes
  void setS3Client(S3Client s3Client) {
    this.s3Client = s3Client;
  }
}
