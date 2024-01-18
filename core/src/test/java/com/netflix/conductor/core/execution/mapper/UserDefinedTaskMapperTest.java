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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.mockito.Mockito.mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserDefinedTaskMapperTest {

    private IDGenerator idGenerator;

    private UserDefinedTaskMapper userDefinedTaskMapper;

    @BeforeEach
    void setUp() {
        ParametersUtils parametersUtils = mock(ParametersUtils.class);
        MetadataDAO metadataDAO = mock(MetadataDAO.class);
        userDefinedTaskMapper = new UserDefinedTaskMapper(parametersUtils, metadataDAO);
        idGenerator = new IDGenerator();
    }

    @Test
    void getMappedTasks() {
        // Given
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("user_task");
        workflowTask.setType(TaskType.USER_DEFINED.name());
        workflowTask.setTaskDefinition(new TaskDef("user_task"));
        String taskId = idGenerator.generate();
        String retriedTaskId = idGenerator.generate();

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
                        .withRetryTaskId(retriedTaskId)
                        .withTaskId(taskId)
                        .build();

        // when
        List<TaskModel> mappedTasks = userDefinedTaskMapper.getMappedTasks(taskMapperContext);

        // Then
        assertEquals(1, mappedTasks.size());
        assertEquals(TaskType.USER_DEFINED.name(), mappedTasks.get(0).getTaskType());
    }

    @Test
    void getMappedTasksException() {
        Throwable exception = assertThrows(TerminateWorkflowException.class, () -> {
            // Given
            WorkflowTask workflowTask = new WorkflowTask();
            workflowTask.setName("user_task");
            workflowTask.setType(TaskType.USER_DEFINED.name());
            String taskId = idGenerator.generate();
            String retriedTaskId = idGenerator.generate();

            WorkflowModel workflow = new WorkflowModel();
            WorkflowDef workflowDef = new WorkflowDef();
            workflow.setWorkflowDefinition(workflowDef);

            TaskMapperContext taskMapperContext =
                    TaskMapperContext.newBuilder()
                            .withWorkflowModel(workflow)
                            .withWorkflowTask(workflowTask)
                            .withTaskInput(new HashMap<>())
                            .withRetryCount(0)
                            .withRetryTaskId(retriedTaskId)
                            .withTaskId(taskId)
                            .build();
            // when
            userDefinedTaskMapper.getMappedTasks(taskMapperContext);
        });
        assertTrue(exception.getMessage().contains(String.format(
                "Invalid task specified. Cannot find task by name %s in the task definitions",
                workflowTask.getName())));
    }
}
