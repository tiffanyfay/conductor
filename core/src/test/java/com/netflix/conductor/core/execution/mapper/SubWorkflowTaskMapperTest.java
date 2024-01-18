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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.workflow.SubWorkflowParams;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_SUB_WORKFLOW;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SubWorkflowTaskMapperTest {

    private SubWorkflowTaskMapper subWorkflowTaskMapper;
    private ParametersUtils parametersUtils;
    private DeciderService deciderService;
    private IDGenerator idGenerator;

    @BeforeEach
    void setUp() {
        parametersUtils = mock(ParametersUtils.class);
        MetadataDAO metadataDAO = mock(MetadataDAO.class);
        subWorkflowTaskMapper = new SubWorkflowTaskMapper(parametersUtils, metadataDAO);
        deciderService = mock(DeciderService.class);
        idGenerator = new IDGenerator();
    }

    @Test
    void getMappedTasks() {
        // Given
        WorkflowDef workflowDef = new WorkflowDef();
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowDefinition(workflowDef);
        WorkflowTask workflowTask = new WorkflowTask();
        SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
        subWorkflowParams.setName("Foo");
        subWorkflowParams.setVersion(2);
        workflowTask.setSubWorkflowParam(subWorkflowParams);
        workflowTask.setStartDelay(30);
        Map<String, Object> taskInput = new HashMap<>();
        Map<String, String> taskToDomain =
                new HashMap<>() {
                    {
                        put("*", "unittest");
                    }
                };

        Map<String, Object> subWorkflowParamMap = new HashMap<>();
        subWorkflowParamMap.put("name", "FooWorkFlow");
        subWorkflowParamMap.put("version", 2);
        subWorkflowParamMap.put("taskToDomain", taskToDomain);
        when(parametersUtils.getTaskInputV2(anyMap(), any(WorkflowModel.class), any(), any()))
                .thenReturn(subWorkflowParamMap);

        // When
        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflowModel)
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(taskInput)
                        .withRetryCount(0)
                        .withTaskId(idGenerator.generate())
                        .withDeciderService(deciderService)
                        .build();

        List<TaskModel> mappedTasks = subWorkflowTaskMapper.getMappedTasks(taskMapperContext);

        // Then
        assertFalse(mappedTasks.isEmpty());
        assertEquals(1, mappedTasks.size());

        TaskModel subWorkFlowTask = mappedTasks.get(0);
        assertEquals(TaskModel.Status.SCHEDULED, subWorkFlowTask.getStatus());
        assertEquals(TASK_TYPE_SUB_WORKFLOW, subWorkFlowTask.getTaskType());
        assertEquals(30, subWorkFlowTask.getCallbackAfterSeconds());
        assertEquals(taskToDomain, subWorkFlowTask.getInputData().get("subWorkflowTaskToDomain"));
    }

    @Test
    void taskToDomain() {
        // Given
        WorkflowDef workflowDef = new WorkflowDef();
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowDefinition(workflowDef);
        WorkflowTask workflowTask = new WorkflowTask();
        Map<String, String> taskToDomain =
                new HashMap<>() {
                    {
                        put("*", "unittest");
                    }
                };
        SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
        subWorkflowParams.setName("Foo");
        subWorkflowParams.setVersion(2);
        subWorkflowParams.setTaskToDomain(taskToDomain);
        workflowTask.setSubWorkflowParam(subWorkflowParams);
        Map<String, Object> taskInput = new HashMap<>();

        Map<String, Object> subWorkflowParamMap = new HashMap<>();
        subWorkflowParamMap.put("name", "FooWorkFlow");
        subWorkflowParamMap.put("version", 2);

        when(parametersUtils.getTaskInputV2(anyMap(), any(WorkflowModel.class), any(), any()))
                .thenReturn(subWorkflowParamMap);

        // When
        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflowModel)
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(taskInput)
                        .withRetryCount(0)
                        .withTaskId(new IDGenerator().generate())
                        .withDeciderService(deciderService)
                        .build();

        List<TaskModel> mappedTasks = subWorkflowTaskMapper.getMappedTasks(taskMapperContext);

        // Then
        assertFalse(mappedTasks.isEmpty());
        assertEquals(1, mappedTasks.size());

        TaskModel subWorkFlowTask = mappedTasks.get(0);
        assertEquals(TaskModel.Status.SCHEDULED, subWorkFlowTask.getStatus());
        assertEquals(TASK_TYPE_SUB_WORKFLOW, subWorkFlowTask.getTaskType());
    }

    @Test
    void getSubWorkflowParams() {
        WorkflowTask workflowTask = new WorkflowTask();
        SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
        subWorkflowParams.setName("Foo");
        subWorkflowParams.setVersion(2);
        workflowTask.setSubWorkflowParam(subWorkflowParams);

        assertEquals(subWorkflowParams, subWorkflowTaskMapper.getSubWorkflowParams(workflowTask));
    }

    @Test
    void getExceptionWhenNoSubWorkflowParamsPassed() {
        Throwable exception = assertThrows(TerminateWorkflowException.class, () -> {
            WorkflowTask workflowTask = new WorkflowTask();
            workflowTask.setName("FooWorkFLow");

            subWorkflowTaskMapper.getSubWorkflowParams(workflowTask);
        });
        assertTrue(exception.getMessage().contains(String.format(
                "Task %s is defined as sub-workflow and is missing subWorkflowParams. "
                        + "Please check the workflow definition",
                workflowTask.getName())));
    }
}
