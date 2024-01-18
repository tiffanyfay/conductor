/*
 * Copyright 2020 Conductor Authors.
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
package com.netflix.conductor.service;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.dao.QueueDAO;

import jakarta.validation.ConstraintViolationException;

import static com.netflix.conductor.TestUtils.getConstraintViolationMessages;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("SpringJavaAutowiredMembersInspection")
@RunWith(SpringRunner.class)
@EnableAutoConfiguration
class TaskServiceTest {

    @TestConfiguration
    static class TestTaskConfiguration {

        @Bean
        public ExecutionService executionService() {
            return mock(ExecutionService.class);
        }

        @Bean
        public TaskService taskService(ExecutionService executionService) {
            QueueDAO queueDAO = mock(QueueDAO.class);
            return new TaskServiceImpl(executionService, queueDAO);
        }
    }

    @Autowired private TaskService taskService;

    @Autowired private ExecutionService executionService;

    @Test
    void poll() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.poll(null, null, null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskType cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void batchPoll() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.batchPoll(null, null, null, null, null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskType cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void getTasks() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.getTasks(null, null, null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskType cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void getPendingTaskForWorkflow() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.getPendingTaskForWorkflow(null, null);
            } catch (ConstraintViolationException ex) {
                assertEquals(2, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("WorkflowId cannot be null or empty."));
                assertTrue(messages.contains("TaskReferenceName cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void updateTask() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.updateTask(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskResult cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void updateTaskInValid() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                TaskResult taskResult = new TaskResult();
                taskService.updateTask(taskResult);
            } catch (ConstraintViolationException ex) {
                assertEquals(2, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("Workflow Id cannot be null or empty"));
                assertTrue(messages.contains("Task ID cannot be null or empty"));
                throw ex;
            }
        });
    }

    @Test
    void ackTaskReceived() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.ackTaskReceived(null, null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskId cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void ackTaskReceivedMissingWorkerId() {
        String ack = taskService.ackTaskReceived("abc", null);
        assertNotNull(ack);
    }

    @Test
    void log() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.log(null, null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskId cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void getTaskLogs() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.getTaskLogs(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskId cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void getTask() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.getTask(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskId cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void removeTaskFromQueue() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.removeTaskFromQueue(null, null);
            } catch (ConstraintViolationException ex) {
                assertEquals(2, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskId cannot be null or empty."));
                assertTrue(messages.contains("TaskType cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void getPollData() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.getPollData(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskType cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void requeuePendingTask() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                taskService.requeuePendingTask(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("TaskType cannot be null or empty."));
                throw ex;
            }
        });
    }

    @Test
    void search() {
        SearchResult<TaskSummary> searchResult =
                new SearchResult<>(2, List.of(mock(TaskSummary.class), mock(TaskSummary.class)));
        when(executionService.getSearchTasks("query", "*", 0, 2, "Sort")).thenReturn(searchResult);
        assertEquals(searchResult, taskService.search(0, 2, "Sort", "*", "query"));
    }

    @Test
    void searchV2() {
        SearchResult<Task> searchResult =
                new SearchResult<>(2, List.of(mock(Task.class), mock(Task.class)));
        when(executionService.getSearchTasksV2("query", "*", 0, 2, "Sort"))
                .thenReturn(searchResult);
        assertEquals(searchResult, taskService.searchV2(0, 2, "Sort", "*", "query"));
    }
}
