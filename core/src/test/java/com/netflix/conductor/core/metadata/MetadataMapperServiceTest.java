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
package com.netflix.conductor.core.metadata;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.SubWorkflowParams;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.dao.MetadataDAO;

import jakarta.validation.ConstraintViolationException;

import static com.netflix.conductor.TestUtils.getConstraintViolationMessages;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("SpringJavaAutowiredMembersInspection")
@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
public class MetadataMapperServiceTest {

    @TestConfiguration
    static class TestMetadataMapperServiceConfiguration {

        @Bean
        public MetadataDAO metadataDAO() {
            return mock(MetadataDAO.class);
        }

        @Bean
        public MetadataMapperService metadataMapperService(MetadataDAO metadataDAO) {
            return new MetadataMapperService(metadataDAO);
        }
    }

    @Autowired private MetadataDAO metadataDAO;

    @Autowired private MetadataMapperService metadataMapperService;

    @AfterEach
    public void cleanUp() {
        reset(metadataDAO);
    }

    @Test
    public void testMetadataPopulationOnSimpleTask() {
        String nameTaskDefinition = "task1";
        TaskDef taskDefinition = createTaskDefinition(nameTaskDefinition);
        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);

        when(metadataDAO.getTaskDef(nameTaskDefinition)).thenReturn(taskDefinition);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(List.of(workflowTask));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        WorkflowTask populatedWorkflowTask = workflowDefinition.getTasks().get(0);
        assertNotNull(populatedWorkflowTask.getTaskDefinition());
        verify(metadataDAO).getTaskDef(nameTaskDefinition);
    }

    @Test
    public void testNoMetadataPopulationOnEmbeddedTaskDefinition() {
        String nameTaskDefinition = "task2";
        TaskDef taskDefinition = createTaskDefinition(nameTaskDefinition);
        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);
        workflowTask.setTaskDefinition(taskDefinition);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(List.of(workflowTask));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        WorkflowTask populatedWorkflowTask = workflowDefinition.getTasks().get(0);
        assertNotNull(populatedWorkflowTask.getTaskDefinition());
        verifyNoInteractions(metadataDAO);
    }

    @Test
    public void testMetadataPopulationOnlyOnNecessaryWorkflowTasks() {
        String nameTaskDefinition1 = "task4";
        TaskDef taskDefinition = createTaskDefinition(nameTaskDefinition1);
        WorkflowTask workflowTask1 = createWorkflowTask(nameTaskDefinition1);
        workflowTask1.setTaskDefinition(taskDefinition);

        String nameTaskDefinition2 = "task5";
        WorkflowTask workflowTask2 = createWorkflowTask(nameTaskDefinition2);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(List.of(workflowTask1, workflowTask2));

        when(metadataDAO.getTaskDef(nameTaskDefinition2)).thenReturn(taskDefinition);

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(2, workflowDefinition.getTasks().size());
        List<WorkflowTask> workflowTasks = workflowDefinition.getTasks();
        assertNotNull(workflowTasks.getFirst().getTaskDefinition());
        assertNotNull(workflowTasks.get(1).getTaskDefinition());

        verify(metadataDAO).getTaskDef(nameTaskDefinition2);
        verifyNoMoreInteractions(metadataDAO);
    }

    @Test
    public void testMetadataPopulationMissingDefinitions() {
        String nameTaskDefinition1 = "task4";
        WorkflowTask workflowTask1 = createWorkflowTask(nameTaskDefinition1);

        String nameTaskDefinition2 = "task5";
        WorkflowTask workflowTask2 = createWorkflowTask(nameTaskDefinition2);

        TaskDef taskDefinition = createTaskDefinition(nameTaskDefinition1);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(List.of(workflowTask1, workflowTask2));

        when(metadataDAO.getTaskDef(nameTaskDefinition1)).thenReturn(taskDefinition);
        when(metadataDAO.getTaskDef(nameTaskDefinition2)).thenReturn(null);

        try {
            metadataMapperService.populateTaskDefinitions(workflowDefinition);
        } catch (NotFoundException nfe) {
            fail("Missing TaskDefinitions are not defaulted");
        }
    }

    @Test
    public void testVersionPopulationForSubworkflowTaskIfVersionIsNotAvailable() {
        String nameTaskDefinition = "taskSubworkflow6";
        String workflowDefinitionName = "subworkflow";
        int version = 3;

        WorkflowDef subWorkflowDefinition = createWorkflowDefinition("workflowDefinitionName");
        subWorkflowDefinition.setVersion(version);

        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);
        workflowTask.setWorkflowTaskType(TaskType.SUB_WORKFLOW);
        SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
        subWorkflowParams.setName(workflowDefinitionName);
        workflowTask.setSubWorkflowParam(subWorkflowParams);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(List.of(workflowTask));

        when(metadataDAO.getLatestWorkflowDef(workflowDefinitionName))
                .thenReturn(Optional.of(subWorkflowDefinition));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        List<WorkflowTask> workflowTasks = workflowDefinition.getTasks();
        SubWorkflowParams params = workflowTasks.getFirst().getSubWorkflowParam();

        assertEquals(workflowDefinitionName, params.getName());
        assertEquals(version, params.getVersion().intValue());

        verify(metadataDAO).getLatestWorkflowDef(workflowDefinitionName);
        verify(metadataDAO).getTaskDef(nameTaskDefinition);
        verifyNoMoreInteractions(metadataDAO);
    }

    @Test
    public void testNoVersionPopulationForSubworkflowTaskIfAvailable() {
        String nameTaskDefinition = "taskSubworkflow7";
        String workflowDefinitionName = "subworkflow";
        Integer version = 2;

        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);
        workflowTask.setWorkflowTaskType(TaskType.SUB_WORKFLOW);
        SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
        subWorkflowParams.setName(workflowDefinitionName);
        subWorkflowParams.setVersion(version);
        workflowTask.setSubWorkflowParam(subWorkflowParams);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(List.of(workflowTask));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        List<WorkflowTask> workflowTasks = workflowDefinition.getTasks();
        SubWorkflowParams params = workflowTasks.getFirst().getSubWorkflowParam();

        assertEquals(workflowDefinitionName, params.getName());
        assertEquals(version, params.getVersion());

        verify(metadataDAO).getTaskDef(nameTaskDefinition);
        verifyNoMoreInteractions(metadataDAO);
    }

    @Test
    public void testExceptionWhenWorkflowDefinitionNotAvailable() {
        assertThrows(TerminateWorkflowException.class, () -> {
            String nameTaskDefinition = "taskSubworkflow8";
            String workflowDefinitionName = "subworkflow";

            WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);
            workflowTask.setWorkflowTaskType(TaskType.SUB_WORKFLOW);
            SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
            subWorkflowParams.setName(workflowDefinitionName);
            workflowTask.setSubWorkflowParam(subWorkflowParams);

            WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
            workflowDefinition.setTasks(List.of(workflowTask));

            when(metadataDAO.getLatestWorkflowDef(workflowDefinitionName)).thenReturn(Optional.empty());

            metadataMapperService.populateTaskDefinitions(workflowDefinition);

            verify(metadataDAO).getLatestWorkflowDef(workflowDefinitionName);
        });
    }

    @Test
    public void testLookupWorkflowDefinition() {
        assertThrows(IllegalArgumentException.class, () -> {
            try {
                String workflowName = "test";
                when(metadataDAO.getWorkflowDef(workflowName, 0))
                        .thenReturn(Optional.of(new WorkflowDef()));
                Optional<WorkflowDef> optionalWorkflowDef =
                        metadataMapperService.lookupWorkflowDefinition(workflowName, 0);
                assertTrue(optionalWorkflowDef.isPresent());
                metadataMapperService.lookupWorkflowDefinition(null, 0);
            } catch (ConstraintViolationException ex) {
                Assertions.assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("WorkflowIds list cannot be null."));
            }
        });
    }

    @Test
    public void testLookupLatestWorkflowDefinition() {
        assertThrows(IllegalArgumentException.class, () -> {
            String workflowName = "test";
            when(metadataDAO.getLatestWorkflowDef(workflowName))
                    .thenReturn(Optional.of(new WorkflowDef()));
            Optional<WorkflowDef> optionalWorkflowDef =
                    metadataMapperService.lookupLatestWorkflowDefinition(workflowName);
            assertTrue(optionalWorkflowDef.isPresent());

            metadataMapperService.lookupLatestWorkflowDefinition(null);
        });
    }

    @Test
    public void testShouldNotPopulateTaskDefinition() {
        WorkflowTask workflowTask = createWorkflowTask("");
        assertFalse(metadataMapperService.shouldPopulateTaskDefinition(workflowTask));
    }

    @Test
    public void testShouldPopulateTaskDefinition() {
        WorkflowTask workflowTask = createWorkflowTask("test");
        assertTrue(metadataMapperService.shouldPopulateTaskDefinition(workflowTask));
    }

    @Test
    public void testMetadataPopulationOnSimpleTaskDefMissing() {
        String nameTaskDefinition = "task1";
        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);

        when(metadataDAO.getTaskDef(nameTaskDefinition)).thenReturn(null);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(List.of(workflowTask));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        WorkflowTask populatedWorkflowTask = workflowDefinition.getTasks().get(0);
        assertNotNull(populatedWorkflowTask.getTaskDefinition());
    }

    private WorkflowDef createWorkflowDefinition(String name) {
        WorkflowDef workflowDefinition = new WorkflowDef();
        workflowDefinition.setName(name);
        return workflowDefinition;
    }

    private WorkflowTask createWorkflowTask(String name) {
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName(name);
        workflowTask.setType(TaskType.SIMPLE.name());
        return workflowTask;
    }

    private TaskDef createTaskDefinition(String name) {
        return new TaskDef(name);
    }
}
