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
package com.netflix.conductor.contribs.tasks.kafka;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class KafkaProducerManagerTest {

    @Test
    void requestTimeoutSetFromDefault() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(100),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        Properties props = manager.getProducerProperties(input);
        assertEquals("100", props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @Test
    void requestTimeoutSetFromInput() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(100),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        input.setRequestTimeoutMs(200);
        Properties props = manager.getProducerProperties(input);
        assertEquals("200", props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @Test
    void requestTimeoutSetFromConfig() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        Properties props = manager.getProducerProperties(input);
        assertEquals("150", props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @SuppressWarnings("rawtypes")
    @Test
    void executionException() {
        assertThrows(RuntimeException.class, () -> {
            KafkaProducerManager manager =
                    new KafkaProducerManager(
                            Duration.ofMillis(150),
                            Duration.ofMillis(500),
                            10,
                            Duration.ofMillis(120000));
            KafkaPublishTask.Input input = getInput();
            Producer producer = manager.getProducer(input);
            assertNotNull(producer);
        });
    }

    @SuppressWarnings("rawtypes")
    @Test
    void cacheInvalidation() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150), Duration.ofMillis(500), 0, Duration.ofMillis(0));
        KafkaPublishTask.Input input = getInput();
        input.setBootStrapServers("");
        Properties props = manager.getProducerProperties(input);
        Producer producerMock = mock(Producer.class);
        Producer producer = manager.getFromCache(props, () -> producerMock);
        assertNotNull(producer);
        verify(producerMock, times(1)).close();
    }

    @Test
    void maxBlockMsFromConfig() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        Properties props = manager.getProducerProperties(input);
        assertEquals("500", props.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @Test
    void maxBlockMsFromInput() {
        KafkaProducerManager manager =
                new KafkaProducerManager(
                        Duration.ofMillis(150),
                        Duration.ofMillis(500),
                        10,
                        Duration.ofMillis(120000));
        KafkaPublishTask.Input input = getInput();
        input.setMaxBlockMs(600);
        Properties props = manager.getProducerProperties(input);
        assertEquals("600", props.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    private KafkaPublishTask.Input getInput() {
        KafkaPublishTask.Input input = new KafkaPublishTask.Input();
        input.setTopic("testTopic");
        input.setValue("TestMessage");
        input.setKeySerializer(LongSerializer.class.getCanonicalName());
        input.setBootStrapServers("servers");
        return input;
    }
}
