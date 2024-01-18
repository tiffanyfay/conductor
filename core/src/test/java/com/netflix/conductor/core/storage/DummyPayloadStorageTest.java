/*
 * Copyright 2023 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.conductor.common.utils.ExternalPayloadStorage.PayloadType;

import static org.junit.jupiter.api.Assertions.*;

public class DummyPayloadStorageTest {

    private DummyPayloadStorage dummyPayloadStorage;

    private static final String TEST_STORAGE_PATH = "test-storage";

    private ExternalStorageLocation location;

    private ObjectMapper objectMapper;

    public static final String MOCK_PAYLOAD = "{\n" + "\"output\": \"TEST_OUTPUT\",\n" + "}\n";

    @BeforeEach
    void setup() {
        dummyPayloadStorage = new DummyPayloadStorage();
        objectMapper = new ObjectMapper();
        location =
                dummyPayloadStorage.getLocation(
                        ExternalPayloadStorage.Operation.WRITE,
                        PayloadType.TASK_OUTPUT,
                        TEST_STORAGE_PATH);
        try {
            byte[] payloadBytes = MOCK_PAYLOAD.getBytes("UTF-8");
            dummyPayloadStorage.upload(
                    location.getPath(),
                    new ByteArrayInputStream(payloadBytes),
                    payloadBytes.length);
        } catch (UnsupportedEncodingException unsupportedEncodingException) {
        }
    }

    @Test
    void getLocationNotNull() {
        assertNotNull(location);
    }

    @Test
    void downloadForValidPath() {
        try (InputStream inputStream = dummyPayloadStorage.download(location.getPath())) {
            Map<String, Object> payload =
                    objectMapper.readValue(
                            IOUtils.toString(inputStream, StandardCharsets.UTF_8), Map.class);
            assertTrue(payload.containsKey("output"));
            assertEquals("TEST_OUTPUT", payload.get("output"));
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
        }
    }

    @Test
    void downloadForInvalidPath() {
        InputStream inputStream = dummyPayloadStorage.download("testPath");
        assertNull(inputStream);
    }
}
