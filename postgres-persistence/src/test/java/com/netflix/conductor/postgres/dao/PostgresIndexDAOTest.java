/*
 * Copyright 2023 Conductor Authors.
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
package com.netflix.conductor.postgres.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.postgres.config.PostgresConfiguration;
import com.netflix.conductor.postgres.util.Query;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;

@ContextConfiguration(
        classes = {
                TestObjectMapperConfiguration.class,
                PostgresConfiguration.class,
                FlywayAutoConfiguration.class
        })
@RunWith(SpringRunner.class)
@TestPropertySource(
        properties = {
                "conductor.app.asyncIndexingEnabled=false",
                "conductor.elasticsearch.version=0",
                "conductor.indexing.type=postgres",
                "spring.flyway.clean-disabled=false"
        })
@SpringBootTest
class PostgresIndexDAOTest {

    @Autowired private PostgresIndexDAO indexDAO;

    @Autowired private ObjectMapper objectMapper;

    @Qualifier("dataSource")
    @Autowired
    private DataSource dataSource;

    @Autowired Flyway flyway;

    // clean the database between tests.
    @BeforeEach
    void before() {
        flyway.migrate();
    }

    private WorkflowSummary getMockWorkflowSummary(String id) {
        WorkflowSummary wfs = new WorkflowSummary();
        wfs.setWorkflowId(id);
        wfs.setCorrelationId("correlation-id");
        wfs.setWorkflowType("workflow-type");
        wfs.setStartTime("2023-02-07T08:42:45Z");
        wfs.setStatus(Workflow.WorkflowStatus.COMPLETED);
        return wfs;
    }

    private TaskSummary getMockTaskSummary(String taskId) {
        TaskSummary ts = new TaskSummary();
        ts.setTaskId(taskId);
        ts.setTaskType("task-type");
        ts.setTaskDefName("task-def-name");
        ts.setStatus(Task.Status.COMPLETED);
        ts.setStartTime("2023-02-07T09:41:45Z");
        ts.setUpdateTime("2023-02-07T09:42:45Z");
        ts.setWorkflowType("workflow-type");
        return ts;
    }

    private TaskExecLog getMockTaskExecutionLog(String taskId, long createdTime, String log) {
        TaskExecLog tse = new TaskExecLog();
        tse.setTaskId(taskId);
        tse.setLog(log);
        tse.setCreatedTime(createdTime);
        return tse;
    }

    private void compareWorkflowSummary(WorkflowSummary wfs) throws SQLException {
        List<Map<String, Object>> result =
                queryDb(
                        String.format(
                                "SELECT * FROM workflow_index WHERE workflow_id = '%s'",
                                wfs.getWorkflowId()));
        assertEquals(1, result.size(), "Wrong number of rows returned");
        assertEquals(
                wfs.getWorkflowId(),
                result.get(0).get("workflow_id"),
                "Workflow id does not match");
        assertEquals(
                wfs.getCorrelationId(),
                result.get(0).get("correlation_id"),
                "Correlation id does not match");
        assertEquals(
                wfs.getWorkflowType(),
                result.get(0).get("workflow_type"),
                "Workflow type does not match");
        TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(wfs.getStartTime());
        Timestamp startTime = Timestamp.from(Instant.from(ta));
        assertEquals(startTime, result.get(0).get("start_time"), "Start time does not match");
        assertEquals(
                wfs.getStatus().toString(), result.get(0).get("status"), "Status does not match");
    }

    private List<Map<String, Object>> queryDb(String query) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            try (Query q = new Query(objectMapper, c, query)) {
                return q.executeAndFetchMap();
            }
        }
    }

    private void compareTaskSummary(TaskSummary ts) throws SQLException {
        List<Map<String, Object>> result =
                queryDb(
                        String.format(
                                "SELECT * FROM task_index WHERE task_id = '%s'", ts.getTaskId()));
        assertEquals(1, result.size(), "Wrong number of rows returned");
        assertEquals(ts.getTaskId(), result.get(0).get("task_id"), "Task id does not match");
        assertEquals(ts.getTaskType(), result.get(0).get("task_type"), "Task type does not match");
        assertEquals(
                ts.getTaskDefName(),
                result.get(0).get("task_def_name"),
                "Task def name does not match");
        TemporalAccessor startTa = DateTimeFormatter.ISO_INSTANT.parse(ts.getStartTime());
        Timestamp startTime = Timestamp.from(Instant.from(startTa));
        assertEquals(startTime, result.get(0).get("start_time"), "Start time does not match");
        TemporalAccessor updateTa = DateTimeFormatter.ISO_INSTANT.parse(ts.getUpdateTime());
        Timestamp updateTime = Timestamp.from(Instant.from(updateTa));
        assertEquals(updateTime, result.get(0).get("update_time"), "Update time does not match");
        assertEquals(
                ts.getStatus().toString(), result.get(0).get("status"), "Status does not match");
        assertEquals(
                ts.getWorkflowType().toString(),
                result.get(0).get("workflow_type"),
                "Workflow type does not match");
    }

    @Test
    void indexNewWorkflow() throws SQLException {
        WorkflowSummary wfs = getMockWorkflowSummary("workflow-id");

        indexDAO.indexWorkflow(wfs);

        compareWorkflowSummary(wfs);
    }

    @Test
    void indexExistingWorkflow() throws SQLException {
        WorkflowSummary wfs = getMockWorkflowSummary("workflow-id");

        indexDAO.indexWorkflow(wfs);

        compareWorkflowSummary(wfs);

        wfs.setStatus(Workflow.WorkflowStatus.FAILED);

        indexDAO.indexWorkflow(wfs);

        compareWorkflowSummary(wfs);
    }

    @Test
    void indexNewTask() throws SQLException {
        TaskSummary ts = getMockTaskSummary("task-id");

        indexDAO.indexTask(ts);

        compareTaskSummary(ts);
    }

    @Test
    void indexExistingTask() throws SQLException {
        TaskSummary ts = getMockTaskSummary("task-id");

        indexDAO.indexTask(ts);

        compareTaskSummary(ts);

        ts.setStatus(Task.Status.FAILED);

        indexDAO.indexTask(ts);

        compareTaskSummary(ts);
    }

    @Test
    void addTaskExecutionLogs() throws SQLException {
        List<TaskExecLog> logs = new ArrayList<>();
        String taskId = UUID.randomUUID().toString();
        logs.add(getMockTaskExecutionLog(taskId, 1675845986000L, "Log 1"));
        logs.add(getMockTaskExecutionLog(taskId, 1675845987000L, "Log 2"));

        indexDAO.addTaskExecutionLogs(logs);

        List<Map<String, Object>> records =
                queryDb("SELECT * FROM task_execution_logs ORDER BY created_time ASC");
        assertEquals(2, records.size(), "Wrong number of logs returned");
        assertEquals(logs.get(0).getLog(), records.get(0).get("log"));
        assertEquals(new Date(1675845986000L), records.get(0).get("created_time"));
        assertEquals(logs.get(1).getLog(), records.get(1).get("log"));
        assertEquals(new Date(1675845987000L), records.get(1).get("created_time"));
    }

    @Test
    void searchWorkflowSummary() {
        WorkflowSummary wfs = getMockWorkflowSummary("workflow-id");

        indexDAO.indexWorkflow(wfs);

        String query = String.format("workflowId=\"%s\"", wfs.getWorkflowId());
        SearchResult<WorkflowSummary> results =
                indexDAO.searchWorkflowSummary(query, "*", 0, 15, new ArrayList());
        assertEquals(1, results.getResults().size(), "No results returned");
        assertEquals(
                wfs.getWorkflowId(),
                results.getResults().get(0).getWorkflowId(),
                "Wrong workflow returned");
    }

    @Test
    void fullTextSearchWorkflowSummary() {
        WorkflowSummary wfs = getMockWorkflowSummary("workflow-id");

        indexDAO.indexWorkflow(wfs);

        String freeText = "notworkflow-id";
        SearchResult<WorkflowSummary> results =
                indexDAO.searchWorkflowSummary("", freeText, 0, 15, new ArrayList());
        assertEquals(0, results.getResults().size(), "Wrong number of results returned");

        freeText = "workflow-id";
        results = indexDAO.searchWorkflowSummary("", freeText, 0, 15, new ArrayList());
        assertEquals(1, results.getResults().size(), "No results returned");
        assertEquals(
                wfs.getWorkflowId(),
                results.getResults().get(0).getWorkflowId(),
                "Wrong workflow returned");
    }

    @Test
    void jsonSearchWorkflowSummary() {
        WorkflowSummary wfs = getMockWorkflowSummary("workflow-id");
        wfs.setVersion(3);

        indexDAO.indexWorkflow(wfs);

        String freeText = "{\"correlationId\":\"not-the-id\"}";
        SearchResult<WorkflowSummary> results =
                indexDAO.searchWorkflowSummary("", freeText, 0, 15, new ArrayList());
        assertEquals(0, results.getResults().size(), "Wrong number of results returned");

        freeText = "{\"correlationId\":\"correlation-id\", \"version\":3}";
        results = indexDAO.searchWorkflowSummary("", freeText, 0, 15, new ArrayList());
        assertEquals(1, results.getResults().size(), "No results returned");
        assertEquals(
                wfs.getWorkflowId(),
                results.getResults().get(0).getWorkflowId(),
                "Wrong workflow returned");
    }

    @Test
    void searchWorkflowSummaryPagination() {
        for (int i = 0; i < 5; i++) {
            WorkflowSummary wfs = getMockWorkflowSummary("workflow-id-" + i);
            indexDAO.indexWorkflow(wfs);
        }

        List<String> orderBy = Arrays.asList(new String[] {"workflowId:DESC"});
        SearchResult<WorkflowSummary> results =
                indexDAO.searchWorkflowSummary("", "*", 0, 2, orderBy);
        assertEquals(3, results.getTotalHits(), "Wrong totalHits returned");
        assertEquals(2, results.getResults().size(), "Wrong number of results returned");
        assertEquals(
                "workflow-id-4",
                results.getResults().get(0).getWorkflowId(),
                "Results returned in wrong order");
        assertEquals(
                "workflow-id-3",
                results.getResults().get(1).getWorkflowId(),
                "Results returned in wrong order");
        results = indexDAO.searchWorkflowSummary("", "*", 2, 2, orderBy);
        assertEquals(5, results.getTotalHits(), "Wrong totalHits returned");
        assertEquals(2, results.getResults().size(), "Wrong number of results returned");
        assertEquals(
                "workflow-id-2",
                results.getResults().get(0).getWorkflowId(),
                "Results returned in wrong order");
        assertEquals(
                "workflow-id-1",
                results.getResults().get(1).getWorkflowId(),
                "Results returned in wrong order");
        results = indexDAO.searchWorkflowSummary("", "*", 4, 2, orderBy);
        assertEquals(7, results.getTotalHits(), "Wrong totalHits returned");
        assertEquals(2, results.getResults().size(), "Wrong number of results returned");
        assertEquals(
                "workflow-id-0",
                results.getResults().get(0).getWorkflowId(),
                "Results returned in wrong order");
    }

    @Test
    void searchTaskSummary() {
        TaskSummary ts = getMockTaskSummary("task-id");

        indexDAO.indexTask(ts);

        String query = String.format("taskId=\"%s\"", ts.getTaskId());
        SearchResult<TaskSummary> results =
                indexDAO.searchTaskSummary(query, "*", 0, 15, new ArrayList());
        assertEquals(1, results.getResults().size(), "No results returned");
        assertEquals(
                ts.getTaskId(), results.getResults().get(0).getTaskId(), "Wrong task returned");
    }

    @Test
    void searchTaskSummaryPagination() {
        for (int i = 0; i < 5; i++) {
            TaskSummary ts = getMockTaskSummary("task-id-" + i);
            indexDAO.indexTask(ts);
        }

        List<String> orderBy = Arrays.asList(new String[] {"taskId:DESC"});
        SearchResult<TaskSummary> results = indexDAO.searchTaskSummary("", "*", 0, 2, orderBy);
        assertEquals(3, results.getTotalHits(), "Wrong totalHits returned");
        assertEquals(2, results.getResults().size(), "Wrong number of results returned");
        assertEquals(
                "task-id-4",
                results.getResults().get(0).getTaskId(),
                "Results returned in wrong order");
        assertEquals(
                "task-id-3",
                results.getResults().get(1).getTaskId(),
                "Results returned in wrong order");
        results = indexDAO.searchTaskSummary("", "*", 2, 2, orderBy);
        assertEquals(5, results.getTotalHits(), "Wrong totalHits returned");
        assertEquals(2, results.getResults().size(), "Wrong number of results returned");
        assertEquals(
                "task-id-2",
                results.getResults().get(0).getTaskId(),
                "Results returned in wrong order");
        assertEquals(
                "task-id-1",
                results.getResults().get(1).getTaskId(),
                "Results returned in wrong order");
        results = indexDAO.searchTaskSummary("", "*", 4, 2, orderBy);
        assertEquals(7, results.getTotalHits(), "Wrong totalHits returned");
        assertEquals(2, results.getResults().size(), "Wrong number of results returned");
        assertEquals(
                "task-id-0",
                results.getResults().get(0).getTaskId(),
                "Results returned in wrong order");
    }

    @Test
    void getTaskExecutionLogs() throws SQLException {
        List<TaskExecLog> logs = new ArrayList<>();
        String taskId = UUID.randomUUID().toString();
        logs.add(getMockTaskExecutionLog(taskId, new Date(1675845986000L).getTime(), "Log 1"));
        logs.add(getMockTaskExecutionLog(taskId, new Date(1675845987000L).getTime(), "Log 2"));

        indexDAO.addTaskExecutionLogs(logs);

        List<TaskExecLog> records = indexDAO.getTaskExecutionLogs(logs.get(0).getTaskId());
        assertEquals(2, records.size(), "Wrong number of logs returned");
        assertEquals(logs.get(0).getLog(), records.get(0).getLog());
        assertEquals(1675845986000L, logs.get(0).getCreatedTime());
        assertEquals(logs.get(1).getLog(), records.get(1).getLog());
        assertEquals(1675845987000L, logs.get(1).getCreatedTime());
    }
}
