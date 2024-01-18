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
package com.netflix.conductor.core.reconciliation;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.TaskModel.Status;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.core.utils.Utils.DECIDER_QUEUE;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TestWorkflowSweeper {

    private ConductorProperties properties;
    private WorkflowExecutor workflowExecutor;
    private WorkflowRepairService workflowRepairService;
    private QueueDAO queueDAO;
    private ExecutionDAOFacade executionDAOFacade;
    private WorkflowSweeper workflowSweeper;

    private int defaultPostPoneOffSetSeconds = 1800;

    @BeforeEach
    void setUp() {
        properties = mock(ConductorProperties.class);
        workflowExecutor = mock(WorkflowExecutor.class);
        queueDAO = mock(QueueDAO.class);
        workflowRepairService = mock(WorkflowRepairService.class);
        executionDAOFacade = mock(ExecutionDAOFacade.class);
        workflowSweeper =
                new WorkflowSweeper(
                        workflowExecutor,
                        Optional.of(workflowRepairService),
                        properties,
                        queueDAO,
                        executionDAOFacade);
    }

    @Test
    void postponeDurationForHumanTaskType() {
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_HUMAN);
        taskModel.setStatus(Status.IN_PROGRESS);
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE,
                        workflowModel.getWorkflowId(),
                        defaultPostPoneOffSetSeconds * 1000);
    }

    @Test
    void postponeDurationForWaitTaskType() {
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_WAIT);
        taskModel.setStatus(Status.IN_PROGRESS);
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE,
                        workflowModel.getWorkflowId(),
                        defaultPostPoneOffSetSeconds * 1000);
    }

    @Test
    void postponeDurationForWaitTaskTypeWithLongWaitTime() {
        long waitTimeout = 65845;
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_WAIT);
        taskModel.setStatus(Status.IN_PROGRESS);
        taskModel.setWaitTimeout(System.currentTimeMillis() + waitTimeout);
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE, workflowModel.getWorkflowId(), (waitTimeout / 1000) * 1000);
    }

    @Test
    void postponeDurationForWaitTaskTypeWithLessOneSecondWaitTime() {
        long waitTimeout = 180;
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_WAIT);
        taskModel.setStatus(Status.IN_PROGRESS);
        taskModel.setWaitTimeout(System.currentTimeMillis() + waitTimeout);
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE, workflowModel.getWorkflowId(), (waitTimeout / 1000) * 1000);
    }

    @Test
    void postponeDurationForWaitTaskTypeWithZeroWaitTime() {
        long waitTimeout = 0;
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_WAIT);
        taskModel.setStatus(Status.IN_PROGRESS);
        taskModel.setWaitTimeout(System.currentTimeMillis() + waitTimeout);
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE, workflowModel.getWorkflowId(), (waitTimeout / 1000) * 1000);
    }

    @Test
    void postponeDurationForTaskInProgress() {
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_SIMPLE);
        taskModel.setStatus(Status.IN_PROGRESS);
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE,
                        workflowModel.getWorkflowId(),
                        defaultPostPoneOffSetSeconds * 1000);
    }

    @Test
    void postponeDurationForTaskInProgressWithResponseTimeoutSet() {
        long responseTimeout = 200;
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_SIMPLE);
        taskModel.setStatus(Status.IN_PROGRESS);
        taskModel.setResponseTimeoutSeconds(responseTimeout);
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE, workflowModel.getWorkflowId(), (responseTimeout + 1) * 1000);
    }

    @Test
    void postponeDurationForTaskInScheduled() {
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowModel.setWorkflowDefinition(workflowDef);
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_SIMPLE);
        taskModel.setStatus(Status.SCHEDULED);
        taskModel.setReferenceTaskName("task1");
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE,
                        workflowModel.getWorkflowId(),
                        defaultPostPoneOffSetSeconds * 1000);
    }

    @Test
    void postponeDurationForTaskInScheduledWithWorkflowTimeoutSet() {
        long workflowTimeout = 1800;
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setTimeoutSeconds(workflowTimeout);
        workflowModel.setWorkflowDefinition(workflowDef);
        TaskModel taskModel = new TaskModel();
        taskModel.setTaskId("task1");
        taskModel.setTaskType(TaskType.TASK_TYPE_SIMPLE);
        taskModel.setStatus(Status.SCHEDULED);
        workflowModel.setTasks(List.of(taskModel));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE, workflowModel.getWorkflowId(), (workflowTimeout + 1) * 1000);
    }

    @Test
    void postponeDurationForTaskInScheduledWithWorkflowTimeoutSetAndNoPollTimeout() {
        long workflowTimeout = 1800;
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setTimeoutSeconds(workflowTimeout);
        workflowModel.setWorkflowDefinition(workflowDef);
        TaskDef taskDef = new TaskDef();
        TaskModel taskModel = mock(TaskModel.class);
        workflowModel.setTasks(List.of(taskModel));
        when(taskModel.getTaskDefinition()).thenReturn(Optional.of(taskDef));
        when(taskModel.getStatus()).thenReturn(Status.SCHEDULED);
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE, workflowModel.getWorkflowId(), (workflowTimeout + 1) * 1000);
    }

    @Test
    void postponeDurationForTaskInScheduledWithNoWorkflowTimeoutSetAndNoPollTimeout() {
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowModel.setWorkflowDefinition(workflowDef);
        TaskDef taskDef = new TaskDef();
        TaskModel taskModel = mock(TaskModel.class);
        workflowModel.setTasks(List.of(taskModel));
        when(taskModel.getTaskDefinition()).thenReturn(Optional.of(taskDef));
        when(taskModel.getStatus()).thenReturn(Status.SCHEDULED);
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE,
                        workflowModel.getWorkflowId(),
                        defaultPostPoneOffSetSeconds * 1000);
    }

    @Test
    void postponeDurationForTaskInScheduledWithNoPollTimeoutSet() {
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskDef taskDef = new TaskDef();
        WorkflowDef workflowDef = new WorkflowDef();
        workflowModel.setWorkflowDefinition(workflowDef);
        TaskModel taskModel = mock(TaskModel.class);
        workflowModel.setTasks(List.of(taskModel));
        when(taskModel.getStatus()).thenReturn(Status.SCHEDULED);
        when(taskModel.getTaskDefinition()).thenReturn(Optional.of(taskDef));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE,
                        workflowModel.getWorkflowId(),
                        defaultPostPoneOffSetSeconds * 1000);
    }

    @Test
    void postponeDurationForTaskInScheduledWithPollTimeoutSet() {
        int pollTimeout = 200;
        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId("1");
        TaskDef taskDef = new TaskDef();
        taskDef.setPollTimeoutSeconds(pollTimeout);
        TaskModel taskModel = mock(TaskModel.class);
        ;
        workflowModel.setTasks(List.of(taskModel));
        when(taskModel.getStatus()).thenReturn(Status.SCHEDULED);
        when(taskModel.getTaskDefinition()).thenReturn(Optional.of(taskDef));
        when(properties.getWorkflowOffsetTimeout())
                .thenReturn(Duration.ofSeconds(defaultPostPoneOffSetSeconds));
        workflowSweeper.unack(workflowModel, defaultPostPoneOffSetSeconds);
        verify(queueDAO)
                .setUnackTimeout(
                        DECIDER_QUEUE, workflowModel.getWorkflowId(), (pollTimeout + 1) * 1000);
    }

    @Test
    void workflowOffsetJitter() {
        long offset = 45;
        for (int i = 0; i < 10; i++) {
            long offsetWithJitter = workflowSweeper.workflowOffsetWithJitter(offset);
            assertTrue(offsetWithJitter >= 30);
            assertTrue(offsetWithJitter <= 60);
        }
    }
}
