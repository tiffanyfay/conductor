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
package com.netflix.conductor.rest.controllers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.service.WorkflowService;
import com.netflix.conductor.service.WorkflowTestService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkflowResourceTest {

    @Mock private WorkflowService mockWorkflowService;

    @Mock private WorkflowTestService mockWorkflowTestService;

    private WorkflowResource workflowResource;

    @BeforeEach
    void before() {
        this.mockWorkflowService = mock(WorkflowService.class);
        this.mockWorkflowTestService = mock(WorkflowTestService.class);
        this.workflowResource =
                new WorkflowResource(this.mockWorkflowService, this.mockWorkflowTestService);
    }

    @Test
    void startWorkflow() {
        StartWorkflowRequest startWorkflowRequest = new StartWorkflowRequest();
        startWorkflowRequest.setName("w123");
        Map<String, Object> input = new HashMap<>();
        input.put("1", "abc");
        startWorkflowRequest.setInput(input);
        String workflowID = "w112";
        when(mockWorkflowService.startWorkflow(any(StartWorkflowRequest.class)))
                .thenReturn(workflowID);
        assertEquals("w112", workflowResource.startWorkflow(startWorkflowRequest));
    }

    @Test
    void startWorkflowParam() {
        Map<String, Object> input = new HashMap<>();
        input.put("1", "abc");
        String workflowID = "w112";
        when(mockWorkflowService.startWorkflow(
                        anyString(), anyInt(), anyString(), anyInt(), anyMap()))
                .thenReturn(workflowID);
        assertEquals("w112", workflowResource.startWorkflow("test1", 1, "c123", 0, input));
    }

    @Test
    void getWorkflows() {
        Workflow workflow = new Workflow();
        workflow.setCorrelationId("123");
        ArrayList<Workflow> listOfWorkflows =
                new ArrayList<>() {
                    {
                        add(workflow);
                    }
                };
        when(mockWorkflowService.getWorkflows(anyString(), anyString(), anyBoolean(), anyBoolean()))
                .thenReturn(listOfWorkflows);
        assertEquals(listOfWorkflows, workflowResource.getWorkflows("test1", "123", true, true));
    }

    @Test
    void getWorklfowsMultipleCorrelationId() {
        Workflow workflow = new Workflow();
        workflow.setCorrelationId("c123");

        List<Workflow> workflowArrayList =
                new ArrayList<>() {
                    {
                        add(workflow);
                    }
                };

        List<String> correlationIdList =
                new ArrayList<>() {
                    {
                        add("c123");
                    }
                };

        Map<String, List<Workflow>> workflowMap = new HashMap<>();
        workflowMap.put("c123", workflowArrayList);

        when(mockWorkflowService.getWorkflows(anyString(), anyBoolean(), anyBoolean(), anyList()))
                .thenReturn(workflowMap);
        assertEquals(
                workflowMap, workflowResource.getWorkflows("test", true, true, correlationIdList));
    }

    @Test
    void getExecutionStatus() {
        Workflow workflow = new Workflow();
        workflow.setCorrelationId("c123");

        when(mockWorkflowService.getExecutionStatus(anyString(), anyBoolean()))
                .thenReturn(workflow);
        assertEquals(workflow, workflowResource.getExecutionStatus("w123", true));
    }

    @Test
    void delete() {
        workflowResource.delete("w123", true);
        verify(mockWorkflowService, times(1)).deleteWorkflow(anyString(), anyBoolean());
    }

    @Test
    void getRunningWorkflow() {
        List<String> listOfWorklfows =
                new ArrayList<>() {
                    {
                        add("w123");
                    }
                };
        when(mockWorkflowService.getRunningWorkflows(anyString(), anyInt(), anyLong(), anyLong()))
                .thenReturn(listOfWorklfows);
        assertEquals(listOfWorklfows, workflowResource.getRunningWorkflow("w123", 1, 12L, 13L));
    }

    @Test
    void decide() {
        workflowResource.decide("w123");
        verify(mockWorkflowService, times(1)).decideWorkflow(anyString());
    }

    @Test
    void pauseWorkflow() {
        workflowResource.pauseWorkflow("w123");
        verify(mockWorkflowService, times(1)).pauseWorkflow(anyString());
    }

    @Test
    void resumeWorkflow() {
        workflowResource.resumeWorkflow("test");
        verify(mockWorkflowService, times(1)).resumeWorkflow(anyString());
    }

    @Test
    void skipTaskFromWorkflow() {
        workflowResource.skipTaskFromWorkflow("test", "testTask", null);
        verify(mockWorkflowService, times(1))
                .skipTaskFromWorkflow(anyString(), anyString(), isNull());
    }

    @Test
    void rerun() {
        RerunWorkflowRequest request = new RerunWorkflowRequest();
        workflowResource.rerun("test", request);
        verify(mockWorkflowService, times(1))
                .rerunWorkflow(anyString(), any(RerunWorkflowRequest.class));
    }

    @Test
    void restart() {
        workflowResource.restart("w123", false);
        verify(mockWorkflowService, times(1)).restartWorkflow(anyString(), anyBoolean());
    }

    @Test
    void retry() {
        workflowResource.retry("w123", false);
        verify(mockWorkflowService, times(1)).retryWorkflow(anyString(), anyBoolean());
    }

    @Test
    void resetWorkflow() {
        workflowResource.resetWorkflow("w123");
        verify(mockWorkflowService, times(1)).resetWorkflow(anyString());
    }

    @Test
    void terminate() {
        workflowResource.terminate("w123", "test");
        verify(mockWorkflowService, times(1)).terminateWorkflow(anyString(), anyString());
    }

    @Test
    void search() {
        workflowResource.search(0, 100, "asc", "*", "*");
        verify(mockWorkflowService, times(1))
                .searchWorkflows(anyInt(), anyInt(), anyString(), anyString(), anyString());
    }

    @Test
    void searchV2() {
        workflowResource.searchV2(0, 100, "asc", "*", "*");
        verify(mockWorkflowService).searchWorkflowsV2(0, 100, "asc", "*", "*");
    }

    @Test
    void searchWorkflowsByTasks() {
        workflowResource.searchWorkflowsByTasks(0, 100, "asc", "*", "*");
        verify(mockWorkflowService, times(1))
                .searchWorkflowsByTasks(anyInt(), anyInt(), anyString(), anyString(), anyString());
    }

    @Test
    void searchWorkflowsByTasksV2() {
        workflowResource.searchWorkflowsByTasksV2(0, 100, "asc", "*", "*");
        verify(mockWorkflowService).searchWorkflowsByTasksV2(0, 100, "asc", "*", "*");
    }

    @Test
    void getExternalStorageLocation() {
        workflowResource.getExternalStorageLocation("path", "operation", "payloadType");
        verify(mockWorkflowService).getExternalStorageLocation("path", "operation", "payloadType");
    }
}
