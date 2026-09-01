/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.amazonaws.kafka.config.providers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

import com.amazonaws.kafka.config.providers.common.CommonConfigUtils;

public class SecretsManagerConfigProviderTest {

    Map<String, Object> props;

    @BeforeEach
    public void setup() {
        props = new HashMap<>();
        props.put("config.providers", "secretsmanager");
        props.put("config.providers.secretsmanager.class",
                "com.amazonaws.kafka.config.providers.MockedSecretsManagerConfigProvider");
        props.put("config.providers.secretsmanager.param.region", "us-west-2");
        props.put("config.providers.secretsmanager.param.NotFoundStrategy", "fail");
    }

    @Test
    public void testExistingKeys() {
        props.put("username", "${secretsmanager:AmazonMSK_TestKafkaConfig:username}");
        props.put("password", "${secretsmanager:AmazonMSK_TestKafkaConfig:password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John", testConfig.getString("username"));
        assertEquals("Password123", testConfig.getString("password"));
    }

    @Test
    public void testExistingKeysViaArn() {
        String arn = URLEncoder.encode(
                "arn:aws:secretsmanager:ap-southeast-2:123456789:secret:AmazonMSK_my_service/my_secret",
                StandardCharsets.UTF_8);
        props.put("username", "${secretsmanager:" + arn + ":username}");
        props.put("password", "${secretsmanager:" + arn + ":password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John2", testConfig.getString("username"));
        assertEquals("Password567", testConfig.getString("password"));
    }

    @Test
    public void testExistingKeysViaArnWithEncodedValue() {
        String arn = URLEncoder.encode(
                "arn:aws:secretsmanager:ap-southeast-2:123456789:secret:AmazonMSK_my_service/my_secret%3A",
                StandardCharsets.UTF_8);
        props.put("username", "${secretsmanager:" + arn + ":username}");
        props.put("password", "${secretsmanager:" + arn + ":password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John3", testConfig.getString("username"));
        assertEquals("Password321", testConfig.getString("password"));
    }

    @Test
    public void testExistingKeysViaHandEncodedArn() {
        String arn = "arn%3Aaws%3Asecretsmanager%3Aap-southeast-2%3A123456789%3Asecret%3AAmazonMSK_my_service%2Fmy_secret";
        props.put("username", "${secretsmanager:" + arn + ":username}");
        props.put("password", "${secretsmanager:" + arn + ":password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John2", testConfig.getString("username"));
        assertEquals("Password567", testConfig.getString("password"));
    }

    @Test
    public void testTtl() {
        props.put("username", "${secretsmanager:AmazonMSK_TestKafkaConfig:username?ttl=60000}");
        props.put("password", "${secretsmanager:AmazonMSK_TestKafkaConfig:password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John", testConfig.getString("username"));
        assertEquals("Password123", testConfig.getString("password"));
    }

    @Test
    public void testNonExistingSecret() {
        props.put("notFound", "${secretsmanager:notFound:noKey}");
        assertThrows(ResourceNotFoundException.class, () -> new CustomConfig(props));
    }

    @Test
    public void testNonExistingKey() {
        props.put("notFound", "${secretsmanager:AmazonMSK_TestKafkaConfig:noKey}");
        assertThrows(ConfigException.class, () -> new CustomConfig(props));
    }

    /**
     * Regression guard for the thread-safety fix: the client must be built once in
     * configure() and reused, not rebuilt on every lookup from a shared mutable builder.
     */
    @Test
    public void testSecretsManagerClientIsBuiltOnceAndReused() {
        SecretsManagerConfigProvider provider = new SecretsManagerConfigProvider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(CommonConfigUtils.REGION, "us-west-2");
        provider.configure(cfg);

        SecretsManagerClient first = provider.checkOrInitSecretManagerClient();
        SecretsManagerClient second = provider.checkOrInitSecretManagerClient();

        assertNotNull(first);
        assertSame(first, second, "client must be built once and reused");
    }

    /**
     * After close() the client is released; the next lookup must lazily rebuild it.
     */
    @Test
    public void testClientReInitializedAfterClose() throws IOException {
        SecretsManagerConfigProvider provider = new SecretsManagerConfigProvider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(CommonConfigUtils.REGION, "us-west-2");
        provider.configure(cfg);

        SecretsManagerClient before = provider.checkOrInitSecretManagerClient();
        provider.close();
        SecretsManagerClient after = provider.checkOrInitSecretManagerClient();

        assertNotNull(after);
        assertNotSame(before, after, "client must be rebuilt after close()");
    }

    /**
     * Concurrency guard: many threads hitting the re-init path at once (after close())
     * must serialize on the synchronized method and end up sharing a single rebuilt
     * client, never building more than one.
     */
    @Test
    public void testConcurrentReInitBuildsSingleClient() throws Exception {
        final SecretsManagerConfigProvider provider = new SecretsManagerConfigProvider();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(CommonConfigUtils.REGION, "us-west-2");
        provider.configure(cfg);
        provider.close(); // null the client so the next lookups take the build path

        final int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch release = new CountDownLatch(1);
        List<Future<SecretsManagerClient>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    release.await(); // line all threads up before the race
                    return provider.checkOrInitSecretManagerClient();
                }));
            }
            release.countDown();

            Set<SecretsManagerClient> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Future<SecretsManagerClient> f : futures) {
                distinct.add(f.get());
            }
            assertEquals(1, distinct.size(), "concurrent callers must share one client instance");
        } finally {
            pool.shutdownNow();
        }
    }

    static class CustomConfig extends AbstractConfig {
        final static String DEFAULT_DOC = "Default Doc";
        final static ConfigDef CONFIG = new ConfigDef()
                .define("username", Type.STRING, "defaultValue", Importance.HIGH, DEFAULT_DOC)
                .define("password", Type.STRING, "defaultValue", Importance.HIGH, DEFAULT_DOC);

        public CustomConfig(Map<?, ?> originals) {
            super(CONFIG, originals);
        }
    }

}
