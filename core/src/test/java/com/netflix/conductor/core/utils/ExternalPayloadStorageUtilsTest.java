/*
 * Copyright 2022 Conductor Authors.
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
package com.netflix.conductor.core.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.unit.DataSize;

import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.conductor.model.TaskModel.Status.FAILED_WITH_TERMINAL_ERROR;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.*;

@SpringJUnitConfig(classes = {TestObjectMapperConfiguration.class})
public class ExternalPayloadStorageUtilsTest {

    private ExternalPayloadStorage externalPayloadStorage;
    private ExternalStorageLocation location;

    @Autowired private ObjectMapper objectMapper;

    // Subject
    private ExternalPayloadStorageUtils externalPayloadStorageUtils;

    @BeforeEach
    public void setup() {
        externalPayloadStorage = mock(ExternalPayloadStorage.class);
        ConductorProperties properties = mock(ConductorProperties.class);
        location = new ExternalStorageLocation();
        location.setPath("some/test/path");

        when(properties.getWorkflowInputPayloadSizeThreshold())
                .thenReturn(DataSize.ofKilobytes(10L));
        when(properties.getMaxWorkflowInputPayloadSizeThreshold())
                .thenReturn(DataSize.ofKilobytes(10240L));
        when(properties.getWorkflowOutputPayloadSizeThreshold())
                .thenReturn(DataSize.ofKilobytes(10L));
        when(properties.getMaxWorkflowOutputPayloadSizeThreshold())
                .thenReturn(DataSize.ofKilobytes(10240L));
        when(properties.getTaskInputPayloadSizeThreshold()).thenReturn(DataSize.ofKilobytes(10L));
        when(properties.getMaxTaskInputPayloadSizeThreshold())
                .thenReturn(DataSize.ofKilobytes(10240L));
        when(properties.getTaskOutputPayloadSizeThreshold()).thenReturn(DataSize.ofKilobytes(10L));
        when(properties.getMaxTaskOutputPayloadSizeThreshold())
                .thenReturn(DataSize.ofKilobytes(10240L));

        externalPayloadStorageUtils =
                new ExternalPayloadStorageUtils(externalPayloadStorage, properties, objectMapper);
    }

    @Test
    public void testDownloadPayload() throws IOException {
        String path = "test/payload";

        Map<String, Object> payload = new HashMap<>();
        payload.put("key1", "value1");
        payload.put("key2", 200);
        byte[] payloadBytes = objectMapper.writeValueAsString(payload).getBytes();
        when(externalPayloadStorage.download(path))
                .thenReturn(new ByteArrayInputStream(payloadBytes));

        Map<String, Object> result = externalPayloadStorageUtils.downloadPayload(path);
        assertNotNull(result);
        assertEquals(payload, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUploadTaskPayload() throws IOException {
        AtomicInteger uploadCount = new AtomicInteger(0);

        InputStream stream =
                com.netflix.conductor.core.utils.ExternalPayloadStorageUtilsTest.class
                        .getResourceAsStream("/payload.json");
        Map<String, Object> payload = objectMapper.readValue(stream, Map.class);

        byte[] payloadBytes = objectMapper.writeValueAsString(payload).getBytes();
        when(externalPayloadStorage.getLocation(
                        ExternalPayloadStorage.Operation.WRITE,
                        ExternalPayloadStorage.PayloadType.TASK_INPUT,
                        "",
                        payloadBytes))
                .thenReturn(location);
        doAnswer(
                        invocation -> {
                            uploadCount.incrementAndGet();
                            return null;
                        })
                .when(externalPayloadStorage)
                .upload(anyString(), any(), anyLong());

        TaskModel task = new TaskModel();
        task.setInputData(payload);
        externalPayloadStorageUtils.verifyAndUpload(
                task, ExternalPayloadStorage.PayloadType.TASK_INPUT);
        assertTrue(StringUtils.isNotEmpty(task.getExternalInputPayloadStoragePath()));
        assertFalse(task.getInputData().isEmpty());
        assertEquals(1, uploadCount.get());
        assertNotNull(task.getExternalInputPayloadStoragePath());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUploadWorkflowPayload() throws IOException {
        AtomicInteger uploadCount = new AtomicInteger(0);

        InputStream stream =
                com.netflix.conductor.core.utils.ExternalPayloadStorageUtilsTest.class
                        .getResourceAsStream("/payload.json");
        Map<String, Object> payload = objectMapper.readValue(stream, Map.class);

        byte[] payloadBytes = objectMapper.writeValueAsString(payload).getBytes();
        when(externalPayloadStorage.getLocation(
                        ExternalPayloadStorage.Operation.WRITE,
                        ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT,
                        "",
                        payloadBytes))
                .thenReturn(location);
        doAnswer(
                        invocation -> {
                            uploadCount.incrementAndGet();
                            return null;
                        })
                .when(externalPayloadStorage)
                .upload(anyString(), any(), anyLong());

        WorkflowModel workflow = new WorkflowModel();
        WorkflowDef def = new WorkflowDef();
        def.setName("name");
        def.setVersion(1);
        workflow.setOutput(payload);
        workflow.setWorkflowDefinition(def);
        externalPayloadStorageUtils.verifyAndUpload(
                workflow, ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT);
        assertTrue(StringUtils.isNotEmpty(workflow.getExternalOutputPayloadStoragePath()));
        assertFalse(workflow.getOutput().isEmpty());
        assertEquals(1, uploadCount.get());
        assertNotNull(workflow.getExternalOutputPayloadStoragePath());
    }

    @Test
    public void testUploadHelper() {
        AtomicInteger uploadCount = new AtomicInteger(0);
        String path = "some/test/path.json";
        ExternalStorageLocation location = new ExternalStorageLocation();
        location.setPath(path);

        when(externalPayloadStorage.getLocation(any(), any(), any(), any())).thenReturn(location);
        doAnswer(
                        invocation -> {
                            uploadCount.incrementAndGet();
                            return null;
                        })
                .when(externalPayloadStorage)
                .upload(anyString(), any(), anyLong());

        assertEquals(
                path,
                externalPayloadStorageUtils.uploadHelper(
                        new byte[] {}, 10L, ExternalPayloadStorage.PayloadType.TASK_OUTPUT));
        assertEquals(1, uploadCount.get());
    }

    @Test
    public void testFailTaskWithInputPayload() {
        TaskModel task = new TaskModel();
        task.setInputData(new HashMap<>());

        externalPayloadStorageUtils.failTask(
                task, ExternalPayloadStorage.PayloadType.TASK_INPUT, "error");
        assertNotNull(task);
        assertTrue(task.getInputData().isEmpty());
        assertEquals(FAILED_WITH_TERMINAL_ERROR, task.getStatus());
    }

    @Test
    public void testFailTaskWithOutputPayload() {
        TaskModel task = new TaskModel();
        task.setOutputData(new HashMap<>());

        externalPayloadStorageUtils.failTask(
                task, ExternalPayloadStorage.PayloadType.TASK_OUTPUT, "error");
        assertNotNull(task);
        assertTrue(task.getOutputData().isEmpty());
        assertEquals(FAILED_WITH_TERMINAL_ERROR, task.getStatus());
    }

    @Test
    public void testFailWorkflowWithInputPayload() {
        assertThrows(TerminateWorkflowException.class, () -> {
            WorkflowModel workflow = new WorkflowModel();
            workflow.setInput(new HashMap<>());
            externalPayloadStorageUtils.failWorkflow(
                    workflow, ExternalPayloadStorage.PayloadType.TASK_INPUT, "error");
            assertNotNull(workflow);
            assertTrue(workflow.getInput().isEmpty());
            assertEquals(WorkflowModel.Status.FAILED, workflow.getStatus());
        });
    }

    @Test
    public void testFailWorkflowWithOutputPayload() {
        assertThrows(TerminateWorkflowException.class, () -> {
            WorkflowModel workflow = new WorkflowModel();
            workflow.setOutput(new HashMap<>());
            externalPayloadStorageUtils.failWorkflow(
                    workflow, ExternalPayloadStorage.PayloadType.TASK_OUTPUT, "error");
            assertNotNull(workflow);
            assertTrue(workflow.getOutput().isEmpty());
            assertEquals(WorkflowModel.Status.FAILED, workflow.getStatus());
        });
    }

    @Test
    public void testShouldUpload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("key1", "value1");
        payload.put("key2", "value2");

        TaskModel task = new TaskModel();
        task.setInputData(payload);
        task.setOutputData(payload);

        WorkflowModel workflow = new WorkflowModel();
        workflow.setInput(payload);
        workflow.setOutput(payload);

        assertTrue(
                externalPayloadStorageUtils.shouldUpload(
                        task, ExternalPayloadStorage.PayloadType.TASK_INPUT));
        assertTrue(
                externalPayloadStorageUtils.shouldUpload(
                        task, ExternalPayloadStorage.PayloadType.TASK_OUTPUT));
        assertTrue(
                externalPayloadStorageUtils.shouldUpload(
                        task, ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT));
        assertTrue(
                externalPayloadStorageUtils.shouldUpload(
                        task, ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT));
    }
}
