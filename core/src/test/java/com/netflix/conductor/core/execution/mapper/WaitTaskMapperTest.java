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
package com.netflix.conductor.core.execution.mapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.conductor.common.metadata.tasks.TaskDef;

import org.junit.jupiter.api.Test;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_WAIT;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class WaitTaskMapperTest {

    @Test
    void getMappedTasks() {

        // Given
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("Wait_task");
        workflowTask.setType(TaskType.WAIT.name());
        String taskId = new IDGenerator().generate();

        ParametersUtils parametersUtils = mock(ParametersUtils.class);
        WorkflowModel workflow = new WorkflowModel();
        WorkflowDef workflowDef = new WorkflowDef();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(new HashMap<>())
                        .withRetryCount(0)
                        .withTaskId(taskId)
                        .build();

        WaitTaskMapper waitTaskMapper = new WaitTaskMapper(parametersUtils);
        // When
        List<TaskModel> mappedTasks = waitTaskMapper.getMappedTasks(taskMapperContext);

        // Then
        assertEquals(1, mappedTasks.size());
        assertEquals(TASK_TYPE_WAIT, mappedTasks.get(0).getTaskType());
    }

    @Test
    void waitForever() {

        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("Wait_task");
        workflowTask.setType(TaskType.WAIT.name());
        String taskId = new IDGenerator().generate();

        ParametersUtils parametersUtils = mock(ParametersUtils.class);
        WorkflowModel workflow = new WorkflowModel();
        WorkflowDef workflowDef = new WorkflowDef();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(new HashMap<>())
                        .withRetryCount(0)
                        .withTaskId(taskId)
                        .build();

        WaitTaskMapper waitTaskMapper = new WaitTaskMapper(parametersUtils);
        // When
        List<TaskModel> mappedTasks = waitTaskMapper.getMappedTasks(taskMapperContext);
        assertEquals(1, mappedTasks.size());
        assertEquals(TaskModel.Status.IN_PROGRESS, mappedTasks.get(0).getStatus());
        assertTrue(mappedTasks.get(0).getOutputData().isEmpty());
    }

    @Test
    void waitUntil() {

        String dateFormat = "yyyy-MM-dd HH:mm";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
        LocalDateTime now = LocalDateTime.now();
        String formatted = formatter.format(now);
        System.out.println(formatted);

        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("Wait_task");
        workflowTask.setType(TaskType.WAIT.name());
        String taskId = new IDGenerator().generate();
        Map<String, Object> input = Map.of(Wait.UNTIL_INPUT, formatted);
        workflowTask.setInputParameters(input);

        ParametersUtils parametersUtils = mock(ParametersUtils.class);
        doReturn(input).when(parametersUtils).getTaskInputV2(any(), any(), any(), any());

        WorkflowModel workflow = new WorkflowModel();
        WorkflowDef workflowDef = new WorkflowDef();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(Map.of(Wait.UNTIL_INPUT, formatted))
                        .withRetryCount(0)
                        .withTaskId(taskId)
                        .build();

        WaitTaskMapper waitTaskMapper = new WaitTaskMapper(parametersUtils);
        // When
        List<TaskModel> mappedTasks = waitTaskMapper.getMappedTasks(taskMapperContext);
        assertEquals(1, mappedTasks.size());
        assertEquals(TaskModel.Status.IN_PROGRESS, mappedTasks.get(0).getStatus());
        assertEquals(0L, mappedTasks.get(0).getCallbackAfterSeconds());
    }

    @Test
    void waitDuration() {

        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("Wait_task");
        workflowTask.setType(TaskType.WAIT.name());
        String taskId = new IDGenerator().generate();
        Map<String, Object> input = Map.of(Wait.DURATION_INPUT, "1s");
        workflowTask.setInputParameters(input);

        ParametersUtils parametersUtils = mock(ParametersUtils.class);
        doReturn(input).when(parametersUtils).getTaskInputV2(any(), any(), any(), any());
        WorkflowModel workflow = new WorkflowModel();
        WorkflowDef workflowDef = new WorkflowDef();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(Map.of(Wait.DURATION_INPUT, "1s"))
                        .withRetryCount(0)
                        .withTaskId(taskId)
                        .build();

        WaitTaskMapper waitTaskMapper = new WaitTaskMapper(parametersUtils);
        // When
        List<TaskModel> mappedTasks = waitTaskMapper.getMappedTasks(taskMapperContext);
        assertEquals(1, mappedTasks.size());
        assertEquals(TaskModel.Status.IN_PROGRESS, mappedTasks.get(0).getStatus());
        assertTrue(mappedTasks.get(0).getCallbackAfterSeconds() <= 1L);
    }

    @Test
    void invalidWaitConfig() {

        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("Wait_task");
        workflowTask.setType(TaskType.WAIT.name());
        String taskId = new IDGenerator().generate();
        Map<String, Object> input =
                Map.of(Wait.DURATION_INPUT, "1s", Wait.UNTIL_INPUT, "2022-12-12");
        workflowTask.setInputParameters(input);

        ParametersUtils parametersUtils = mock(ParametersUtils.class);
        doReturn(input).when(parametersUtils).getTaskInputV2(any(), any(), any(), any());
        WorkflowModel workflow = new WorkflowModel();
        WorkflowDef workflowDef = new WorkflowDef();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(
                                Map.of(Wait.DURATION_INPUT, "1s", Wait.UNTIL_INPUT, "2022-12-12"))
                        .withRetryCount(0)
                        .withTaskId(taskId)
                        .build();

        WaitTaskMapper waitTaskMapper = new WaitTaskMapper(parametersUtils);
        // When
        List<TaskModel> mappedTasks = waitTaskMapper.getMappedTasks(taskMapperContext);
        assertEquals(1, mappedTasks.size());
        assertEquals(TaskModel.Status.FAILED_WITH_TERMINAL_ERROR, mappedTasks.get(0).getStatus());
    }
}
