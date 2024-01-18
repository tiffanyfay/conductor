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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.netflix.conductor.core.execution.WorkflowExecutor;

import jakarta.validation.ConstraintViolationException;

import static com.netflix.conductor.TestUtils.getConstraintViolationMessages;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("SpringJavaAutowiredMembersInspection")
@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
public class WorkflowBulkServiceTest {

    @TestConfiguration
    static class TestWorkflowBulkConfiguration {

        @Bean
        WorkflowExecutor workflowExecutor() {
            return mock(WorkflowExecutor.class);
        }

        @Bean
        public WorkflowBulkService workflowBulkService(WorkflowExecutor workflowExecutor) {
            return new WorkflowBulkServiceImpl(workflowExecutor);
        }
    }

    @Autowired private WorkflowExecutor workflowExecutor;

    @Autowired private WorkflowBulkService workflowBulkService;

    @Test
    public void testPauseWorkflowNull() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                workflowBulkService.pauseWorkflow(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("WorkflowIds list cannot be null."));
                throw ex;
            }
        });
    }

    @Test
    public void testPauseWorkflowWithInvalidListSize() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                List<String> list = new ArrayList<>(1001);
                for (int i = 0; i < 1002; i++) {
                    list.add("test");
                }
                workflowBulkService.pauseWorkflow(list);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(
                        messages.contains(
                                "Cannot process more than 1000 workflows. Please use multiple requests."));
                throw ex;
            }
        });
    }

    @Test
    public void testResumeWorkflowNull() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                workflowBulkService.resumeWorkflow(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("WorkflowIds list cannot be null."));
                throw ex;
            }
        });
    }

    @Test
    public void testRestartWorkflowNull() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                workflowBulkService.restart(null, false);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("WorkflowIds list cannot be null."));
                throw ex;
            }
        });
    }

    @Test
    public void testRetryWorkflowNull() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                workflowBulkService.retry(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("WorkflowIds list cannot be null."));
                throw ex;
            }
        });
    }

    @Test
    public void testRetryWorkflowSuccessful() {
        // When
        workflowBulkService.retry(Collections.singletonList("anyId"));
        // Then
        verify(workflowExecutor).retry("anyId", false);
    }

    @Test
    public void testTerminateNull() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                workflowBulkService.terminate(null, null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("WorkflowIds list cannot be null."));
                throw ex;
            }
        });
    }
}
