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

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.run.*;
import com.netflix.conductor.service.WorkflowService;
import com.netflix.conductor.service.WorkflowTestService;

import io.swagger.v3.oas.annotations.Operation;

import static com.netflix.conductor.rest.config.RequestMappingConstants.WORKFLOW;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController
@RequestMapping(WORKFLOW)
public class WorkflowResource {

    private final WorkflowService workflowService;

    private final WorkflowTestService workflowTestService;

    public WorkflowResource(
            WorkflowService workflowService, WorkflowTestService workflowTestService) {
        this.workflowService = workflowService;
        this.workflowTestService = workflowTestService;
    }

    @PostMapping(produces = TEXT_PLAIN_VALUE)
    @Operation(
            summary =
                    "Start a new workflow with StartWorkflowRequest, which allows task to be executed in a domain")
    public String startWorkflow(@RequestBody StartWorkflowRequest request) {
        return workflowService.startWorkflow(request);
    }

    @PostMapping(value = "/{name}", produces = TEXT_PLAIN_VALUE)
    @Operation(
            summary =
                    "Start a new workflow. Returns the ID of the workflow instance that can be later used for tracking")
    public String startWorkflow(
            @PathVariable String name,
            @RequestParam(required = false) Integer version,
            @RequestParam(required = false) String correlationId,
            @RequestParam(defaultValue = "0", required = false) int priority,
            @RequestBody Map<String, Object> input) {
        return workflowService.startWorkflow(name, version, correlationId, priority, input);
    }

    @GetMapping("/{name}/correlated/{correlationId}")
    @Operation(summary = "Lists workflows for the given correlation id")
    public List<Workflow> getWorkflows(
            @PathVariable String name,
            @PathVariable String correlationId,
            @RequestParam(defaultValue = "false", required = false)
                    boolean includeClosed,
            @RequestParam(defaultValue = "false", required = false)
                    boolean includeTasks) {
        return workflowService.getWorkflows(name, correlationId, includeClosed, includeTasks);
    }

    @PostMapping(value = "/{name}/correlated")
    @Operation(summary = "Lists workflows for the given correlation id list")
    public Map<String, List<Workflow>> getWorkflows(
            @PathVariable String name,
            @RequestParam(defaultValue = "false", required = false)
                    boolean includeClosed,
            @RequestParam(defaultValue = "false", required = false)
                    boolean includeTasks,
            @RequestBody List<String> correlationIds) {
        return workflowService.getWorkflows(name, includeClosed, includeTasks, correlationIds);
    }

    @GetMapping("/{workflowId}")
    @Operation(summary = "Gets the workflow by workflow id")
    public Workflow getExecutionStatus(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "true", required = false)
                    boolean includeTasks) {
        return workflowService.getExecutionStatus(workflowId, includeTasks);
    }

    @DeleteMapping("/{workflowId}/remove")
    @Operation(summary = "Removes the workflow from the system")
    public void delete(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "true", required = false)
                    boolean archiveWorkflow) {
        workflowService.deleteWorkflow(workflowId, archiveWorkflow);
    }

    @GetMapping("/running/{name}")
    @Operation(summary = "Retrieve all the running workflows")
    public List<String> getRunningWorkflow(
            @PathVariable("name") String workflowName,
            @RequestParam(defaultValue = "1", required = false) int version,
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {
        return workflowService.getRunningWorkflows(workflowName, version, startTime, endTime);
    }

    @PutMapping("/decide/{workflowId}")
    @Operation(summary = "Starts the decision task for a workflow")
    public void decide(@PathVariable String workflowId) {
        workflowService.decideWorkflow(workflowId);
    }

    @PutMapping("/{workflowId}/pause")
    @Operation(summary = "Pauses the workflow")
    public void pauseWorkflow(@PathVariable String workflowId) {
        workflowService.pauseWorkflow(workflowId);
    }

    @PutMapping("/{workflowId}/resume")
    @Operation(summary = "Resumes the workflow")
    public void resumeWorkflow(@PathVariable String workflowId) {
        workflowService.resumeWorkflow(workflowId);
    }

    @PutMapping("/{workflowId}/skiptask/{taskReferenceName}")
    @Operation(summary = "Skips a given task from a current running workflow")
    public void skipTaskFromWorkflow(
            @PathVariable String workflowId,
            @PathVariable String taskReferenceName,
            SkipTaskRequest skipTaskRequest) {
        workflowService.skipTaskFromWorkflow(workflowId, taskReferenceName, skipTaskRequest);
    }

    @PostMapping(value = "/{workflowId}/rerun", produces = TEXT_PLAIN_VALUE)
    @Operation(summary = "Reruns the workflow from a specific task")
    public String rerun(
            @PathVariable String workflowId,
            @RequestBody RerunWorkflowRequest request) {
        return workflowService.rerunWorkflow(workflowId, request);
    }

    @PostMapping("/{workflowId}/restart")
    @Operation(summary = "Restarts a completed workflow")
    @ResponseStatus(
            value = HttpStatus.NO_CONTENT) // for backwards compatibility with 2.x client which
    // expects a 204 for this request
    public void restart(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "false", required = false)
                    boolean useLatestDefinitions) {
        workflowService.restartWorkflow(workflowId, useLatestDefinitions);
    }

    @PostMapping("/{workflowId}/retry")
    @Operation(summary = "Retries the last failed task")
    @ResponseStatus(
            value = HttpStatus.NO_CONTENT) // for backwards compatibility with 2.x client which
    // expects a 204 for this request
    public void retry(
            @PathVariable String workflowId,
            @RequestParam(
                            defaultValue = "false",
                            required = false)
                    boolean resumeSubworkflowTasks) {
        workflowService.retryWorkflow(workflowId, resumeSubworkflowTasks);
    }

    @PostMapping("/{workflowId}/resetcallbacks")
    @Operation(summary = "Resets callback times of all non-terminal SIMPLE tasks to 0")
    @ResponseStatus(
            value = HttpStatus.NO_CONTENT) // for backwards compatibility with 2.x client which
    // expects a 204 for this request
    public void resetWorkflow(@PathVariable String workflowId) {
        workflowService.resetWorkflow(workflowId);
    }

    @DeleteMapping("/{workflowId}")
    @Operation(summary = "Terminate workflow execution")
    public void terminate(
            @PathVariable String workflowId,
            @RequestParam(required = false) String reason) {
        workflowService.terminateWorkflow(workflowId, reason);
    }

    @Operation(
            summary = "Search for workflows based on payload and other parameters",
            description =
                    """
                    use sort options as sort=<field>:ASC|DESC e.g. sort=name&sort=workflowId:DESC.\
                     If order is not specified, defaults to ASC.\
                    """)
    @GetMapping(value = "/search")
    public SearchResult<WorkflowSummary> search(
            @RequestParam(defaultValue = "0", required = false) int start,
            @RequestParam(defaultValue = "100", required = false) int size,
            @RequestParam(required = false) String sort,
            @RequestParam(defaultValue = "*", required = false) String freeText,
            @RequestParam(required = false) String query) {
        return workflowService.searchWorkflows(start, size, sort, freeText, query);
    }

    @Operation(
            summary = "Search for workflows based on payload and other parameters",
            description =
                    """
                    use sort options as sort=<field>:ASC|DESC e.g. sort=name&sort=workflowId:DESC.\
                     If order is not specified, defaults to ASC.\
                    """)
    @GetMapping(value = "/search-v2")
    public SearchResult<Workflow> searchV2(
            @RequestParam(defaultValue = "0", required = false) int start,
            @RequestParam(defaultValue = "100", required = false) int size,
            @RequestParam(required = false) String sort,
            @RequestParam(defaultValue = "*", required = false) String freeText,
            @RequestParam(required = false) String query) {
        return workflowService.searchWorkflowsV2(start, size, sort, freeText, query);
    }

    @Operation(
            summary = "Search for workflows based on task parameters",
            description =
                    """
                    use sort options as sort=<field>:ASC|DESC e.g. sort=name&sort=workflowId:DESC.\
                     If order is not specified, defaults to ASC\
                    """)
    @GetMapping(value = "/search-by-tasks")
    public SearchResult<WorkflowSummary> searchWorkflowsByTasks(
            @RequestParam(defaultValue = "0", required = false) int start,
            @RequestParam(defaultValue = "100", required = false) int size,
            @RequestParam(required = false) String sort,
            @RequestParam(defaultValue = "*", required = false) String freeText,
            @RequestParam(required = false) String query) {
        return workflowService.searchWorkflowsByTasks(start, size, sort, freeText, query);
    }

    @Operation(
            summary = "Search for workflows based on task parameters",
            description =
                    """
                    use sort options as sort=<field>:ASC|DESC e.g. sort=name&sort=workflowId:DESC.\
                     If order is not specified, defaults to ASC\
                    """)
    @GetMapping(value = "/search-by-tasks-v2")
    public SearchResult<Workflow> searchWorkflowsByTasksV2(
            @RequestParam(defaultValue = "0", required = false) int start,
            @RequestParam(defaultValue = "100", required = false) int size,
            @RequestParam(required = false) String sort,
            @RequestParam(defaultValue = "*", required = false) String freeText,
            @RequestParam(required = false) String query) {
        return workflowService.searchWorkflowsByTasksV2(start, size, sort, freeText, query);
    }

    @Operation(
            summary =
                    "Get the uri and path of the external storage where the workflow payload is to be stored")
    @GetMapping({"/externalstoragelocation", "external-storage-location"})
    public ExternalStorageLocation getExternalStorageLocation(
            @RequestParam String path,
            @RequestParam String operation,
            @RequestParam String payloadType) {
        return workflowService.getExternalStorageLocation(path, operation, payloadType);
    }

    @PostMapping(value = "test", produces = APPLICATION_JSON_VALUE)
    @Operation(summary = "Test workflow execution using mock data")
    public Workflow testWorkflow(@RequestBody WorkflowTestRequest request) {
        return workflowTestService.testWorkflow(request);
    }
}
