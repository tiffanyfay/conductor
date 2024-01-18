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
package com.netflix.conductor.core.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class QueueUtilsTest {

    @Test
    void queueNameWithTypeAndIsolationGroup() {
        String queueNameGenerated = QueueUtils.getQueueName("tType", null, "isolationGroup", null);
        String queueNameGeneratedOnlyType = QueueUtils.getQueueName("tType", null, null, null);
        String queueNameGeneratedWithAllValues =
                QueueUtils.getQueueName("tType", "domain", "iso", "eN");

        Assertions.assertEquals("tType-isolationGroup", queueNameGenerated);
        Assertions.assertEquals("tType", queueNameGeneratedOnlyType);
        Assertions.assertEquals("domain:tType@eN-iso", queueNameGeneratedWithAllValues);
    }

    @Test
    void notIsolatedIfSeparatorNotPresent() {
        String notIsolatedQueue = "notIsolated";
        Assertions.assertFalse(QueueUtils.isIsolatedQueue(notIsolatedQueue));
    }

    @Test
    void getExecutionNameSpace() {
        String executionNameSpace = QueueUtils.getExecutionNameSpace("domain:queueName@eN-iso");
        Assertions.assertEquals("eN", executionNameSpace);
    }

    @Test
    void getQueueExecutionNameSpaceEmpty() {
        Assertions.assertEquals("", QueueUtils.getExecutionNameSpace("queueName"));
    }

    @Test
    void getQueueExecutionNameSpaceWithIsolationGroup() {
        Assertions.assertEquals(
                "executionNameSpace",
                QueueUtils.getExecutionNameSpace("domain:test@executionNameSpace-isolated"));
    }

    @Test
    void getQueueName() {
        Assertions.assertEquals(
                "domain:taskType@eN-isolated",
                QueueUtils.getQueueName("taskType", "domain", "isolated", "eN"));
    }

    @Test
    void getTaskType() {
        Assertions.assertEquals("taskType", QueueUtils.getTaskType("domain:taskType-isolated"));
    }

    @Test
    void getTaskTypeWithoutDomain() {
        Assertions.assertEquals("taskType", QueueUtils.getTaskType("taskType-isolated"));
    }

    @Test
    void getTaskTypeWithoutDomainAndWithoutIsolationGroup() {
        Assertions.assertEquals("taskType", QueueUtils.getTaskType("taskType"));
    }

    @Test
    void getTaskTypeWithoutDomainAndWithExecutionNameSpace() {
        Assertions.assertEquals("taskType", QueueUtils.getTaskType("taskType@eN"));
    }
}
