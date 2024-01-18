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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.postgres.config.PostgresConfiguration;
import com.netflix.conductor.postgres.util.Query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import static org.junit.jupiter.api.Assertions.*;

@ContextConfiguration(
        classes = {
            TestObjectMapperConfiguration.class,
            PostgresConfiguration.class,
            FlywayAutoConfiguration.class
        })
@RunWith(SpringRunner.class)
@SpringBootTest(properties = "spring.flyway.clean-disabled=false")
public class PostgresQueueDAOTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresQueueDAOTest.class);

    @Autowired private PostgresQueueDAO queueDAO;

    @Qualifier("dataSource")
    @Autowired
    private DataSource dataSource;

    @Autowired private ObjectMapper objectMapper;

     public String name;

    @Autowired Flyway flyway;

    // clean the database between tests.
    @BeforeEach
    void before(TestInfo testInfo) {
        Optional<Method> testMethod = testInfo.getTestMethod();
        if (testMethod.isPresent()) {
            this.name = testMethod.get().getName();
        }
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(true);
            String[] stmts =
                    new String[]{"truncate table queue;", "truncate table queue_message;"};
            for (String stmt : stmts) {
                conn.prepareStatement(stmt).executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    void complexQueueTest() {
        String queueName = "TestQueue";
        long offsetTimeInSecond = 0;

        for (int i = 0; i < 10; i++) {
            String messageId = "msg" + i;
            queueDAO.push(queueName, messageId, offsetTimeInSecond);
        }
        int size = queueDAO.getSize(queueName);
        assertEquals(10, size);
        Map<String, Long> details = queueDAO.queuesDetail();
        assertEquals(1, details.size());
        assertEquals(10L, details.get(queueName).longValue());

        for (int i = 0; i < 10; i++) {
            String messageId = "msg" + i;
            queueDAO.pushIfNotExists(queueName, messageId, offsetTimeInSecond);
        }

        List<String> popped = queueDAO.pop(queueName, 10, 100);
        assertNotNull(popped);
        assertEquals(10, popped.size());

        Map<String, Map<String, Map<String, Long>>> verbose = queueDAO.queuesDetailVerbose();
        assertEquals(1, verbose.size());
        long shardSize = verbose.get(queueName).get("a").get("size");
        long unackedSize = verbose.get(queueName).get("a").get("uacked");
        assertEquals(0, shardSize);
        assertEquals(10, unackedSize);

        popped.forEach(messageId -> queueDAO.ack(queueName, messageId));

        verbose = queueDAO.queuesDetailVerbose();
        assertEquals(1, verbose.size());
        shardSize = verbose.get(queueName).get("a").get("size");
        unackedSize = verbose.get(queueName).get("a").get("uacked");
        assertEquals(0, shardSize);
        assertEquals(0, unackedSize);

        popped = queueDAO.pop(queueName, 10, 100);
        assertNotNull(popped);
        assertEquals(0, popped.size());

        for (int i = 0; i < 10; i++) {
            String messageId = "msg" + i;
            queueDAO.pushIfNotExists(queueName, messageId, offsetTimeInSecond);
        }
        size = queueDAO.getSize(queueName);
        assertEquals(10, size);

        for (int i = 0; i < 10; i++) {
            String messageId = "msg" + i;
            assertTrue(queueDAO.containsMessage(queueName, messageId));
            queueDAO.remove(queueName, messageId);
        }

        size = queueDAO.getSize(queueName);
        assertEquals(0, size);

        for (int i = 0; i < 10; i++) {
            String messageId = "msg" + i;
            queueDAO.pushIfNotExists(queueName, messageId, offsetTimeInSecond);
        }
        queueDAO.flush(queueName);
        size = queueDAO.getSize(queueName);
        assertEquals(0, size);
    }

    /**
     * Test fix for https://github.com/Netflix/conductor/issues/399
     *
     * @since 1.8.2-rc5
     */
    @Test
    void pollMessagesTest() {
        final List<Message> messages = new ArrayList<>();
        final String queueName = "issue399_testQueue";
        final int totalSize = 10;

        for (int i = 0; i < totalSize; i++) {
            String payload = "{\"id\": " + i + ", \"msg\":\"test " + i + "\"}";
            Message m = new Message("testmsg-" + i, payload, "");
            if (i % 2 == 0) {
                // Set priority on message with pair id
                m.setPriority(99 - i);
            }
            messages.add(m);
        }

        // Populate the queue with our test message batch
        queueDAO.push(queueName, ImmutableList.copyOf(messages));

        // Assert that all messages were persisted and no extras are in there
        assertEquals(totalSize, queueDAO.getSize(queueName), "Queue size mismatch");

        List<Message> zeroPoll = queueDAO.pollMessages(queueName, 0, 10_000);
        assertTrue(zeroPoll.isEmpty(), "Zero poll should be empty");

        final int firstPollSize = 3;
        List<Message> firstPoll = queueDAO.pollMessages(queueName, firstPollSize, 10_000);
        assertNotNull(firstPoll, "First poll was null");
        assertFalse(firstPoll.isEmpty(), "First poll was empty");
        assertEquals(firstPollSize, firstPoll.size(), "First poll size mismatch");

        final int secondPollSize = 4;
        List<Message> secondPoll = queueDAO.pollMessages(queueName, secondPollSize, 10_000);
        assertNotNull(secondPoll, "Second poll was null");
        assertFalse(secondPoll.isEmpty(), "Second poll was empty");
        assertEquals(secondPollSize, secondPoll.size(), "Second poll size mismatch");

        // Assert that the total queue size hasn't changed
        assertEquals(
                totalSize,
                queueDAO.getSize(queueName),
                "Total queue size should have remained the same");

        // Assert that our un-popped messages match our expected size
        final long expectedSize = totalSize - firstPollSize - secondPollSize;
        try (Connection c = dataSource.getConnection()) {
            String UNPOPPED =
                    "SELECT COUNT(*) FROM queue_message WHERE queue_name = ? AND popped = false";
            try (Query q = new Query(objectMapper, c, UNPOPPED)) {
                long count = q.addParameter(queueName).executeCount();
                assertEquals(expectedSize, count, "Remaining queue size mismatch");
            }
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /** Test fix for https://github.com/Netflix/conductor/issues/1892 */
    @Test
    void containsMessageTest() {
        String queueName = "TestQueue";
        long offsetTimeInSecond = 0;

        for (int i = 0; i < 10; i++) {
            String messageId = "msg" + i;
            queueDAO.push(queueName, messageId, offsetTimeInSecond);
        }
        int size = queueDAO.getSize(queueName);
        assertEquals(10, size);

        for (int i = 0; i < 10; i++) {
            String messageId = "msg" + i;
            assertTrue(queueDAO.containsMessage(queueName, messageId));
            queueDAO.remove(queueName, messageId);
        }
        for (int i = 0; i < 10; i++) {
            String messageId = "msg" + i;
            assertFalse(queueDAO.containsMessage(queueName, messageId));
        }
    }

    /**
     * Test fix for https://github.com/Netflix/conductor/issues/448
     *
     * @since 1.8.2-rc5
     */
    @Test
    void pollDeferredMessagesTest() throws InterruptedException {
        final List<Message> messages = new ArrayList<>();
        final String queueName = "issue448_testQueue";
        final int totalSize = 10;

        for (int i = 0; i < totalSize; i++) {
            int offset = 0;
            if (i < 5) {
                offset = 0;
            } else if (i == 6 || i == 7) {
                // Purposefully skipping id:5 to test out of order deliveries
                // Set id:6 and id:7 for a 2s delay to be picked up in the second polling batch
                offset = 5;
            } else {
                // Set all other queue messages to have enough of a delay that they won't
                // accidentally
                // be picked up.
                offset = 10_000 + i;
            }

            String payload = "{\"id\": " + i + ",\"offset_time_seconds\":" + offset + "}";
            Message m = new Message("testmsg-" + i, payload, "");
            messages.add(m);
            queueDAO.push(queueName, "testmsg-" + i, offset);
        }

        // Assert that all messages were persisted and no extras are in there
        assertEquals(totalSize, queueDAO.getSize(queueName), "Queue size mismatch");

        final int firstPollSize = 4;
        List<Message> firstPoll = queueDAO.pollMessages(queueName, firstPollSize, 100);
        assertNotNull(firstPoll, "First poll was null");
        assertFalse(firstPoll.isEmpty(), "First poll was empty");
        assertEquals(firstPollSize, firstPoll.size(), "First poll size mismatch");

        List<String> firstPollMessageIds =
                messages.stream()
                        .map(Message::getId)
                        .collect(Collectors.toList())
                        .subList(0, firstPollSize + 1);

        for (int i = 0; i < firstPollSize; i++) {
            String actual = firstPoll.get(i).getId();
            assertTrue(firstPollMessageIds.contains(actual), "Unexpected Id: " + actual);
        }

        final int secondPollSize = 3;

        // Sleep a bit to get the next batch of messages
        LOGGER.info("Sleeping for second poll...");
        Thread.sleep(5_000);

        // Poll for many more messages than expected
        List<Message> secondPoll = queueDAO.pollMessages(queueName, secondPollSize + 10, 100);
        assertNotNull(secondPoll, "Second poll was null");
        assertFalse(secondPoll.isEmpty(), "Second poll was empty");
        assertEquals(secondPollSize, secondPoll.size(), "Second poll size mismatch");

        List<String> expectedIds = Arrays.asList("testmsg-4", "testmsg-6", "testmsg-7");
        for (int i = 0; i < secondPollSize; i++) {
            String actual = secondPoll.get(i).getId();
            assertTrue(expectedIds.contains(actual), "Unexpected Id: " + actual);
        }

        // Assert that the total queue size hasn't changed
        assertEquals(
                totalSize,
                queueDAO.getSize(queueName),
                "Total queue size should have remained the same");

        // Assert that our un-popped messages match our expected size
        final long expectedSize = totalSize - firstPollSize - secondPollSize;
        try (Connection c = dataSource.getConnection()) {
            String UNPOPPED =
                    "SELECT COUNT(*) FROM queue_message WHERE queue_name = ? AND popped = false";
            try (Query q = new Query(objectMapper, c, UNPOPPED)) {
                long count = q.addParameter(queueName).executeCount();
                assertEquals(expectedSize, count, "Remaining queue size mismatch");
            }
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    // @Test
    public void processUnacksTest() {
        processUnacks(
                () -> {
                    // Process unacks
                    queueDAO.processUnacks("process_unacks_test");
                },
                "process_unacks_test");
    }

    // @Test
    public void processAllUnacksTest() {
        processUnacks(
                () -> {
                    // Process all unacks
                    queueDAO.processAllUnacks();
                },
                "process_unacks_test");
    }

    private void processUnacks(Runnable unack, String queueName) {
        // Count of messages in the queue(s)
        final int count = 10;
        // Number of messages to process acks for
        final int unackedCount = 4;
        // A secondary queue to make sure we don't accidentally process other queues
        final String otherQueueName = "process_unacks_test_other_queue";

        // Create testing queue with some messages (but not all) that will be popped/acked.
        for (int i = 0; i < count; i++) {
            int offset = 0;
            if (i >= unackedCount) {
                offset = 1_000_000;
            }

            queueDAO.push(queueName, "unack-" + i, offset);
        }

        // Create a second queue to make sure that unacks don't occur for it
        for (int i = 0; i < count; i++) {
            queueDAO.push(otherQueueName, "other-" + i, 0);
        }

        // Poll for first batch of messages (should be equal to unackedCount)
        List<Message> polled = queueDAO.pollMessages(queueName, 100, 10_000);
        assertNotNull(polled);
        assertFalse(polled.isEmpty());
        assertEquals(unackedCount, polled.size());

        // Poll messages from the other queue so we know they don't get unacked later
        queueDAO.pollMessages(otherQueueName, 100, 10_000);

        // Ack one of the polled messages
        assertTrue(queueDAO.ack(queueName, "unack-1"));

        // Should have one less un-acked popped message in the queue
        Long uacked = queueDAO.queuesDetailVerbose().get(queueName).get("a").get("uacked");
        assertNotNull(uacked);
        assertEquals(uacked.longValue(), unackedCount - 1);

        unack.run();

        // Check uacks for both queues after processing
        Map<String, Map<String, Map<String, Long>>> details = queueDAO.queuesDetailVerbose();
        uacked = details.get(queueName).get("a").get("uacked");
        assertNotNull(uacked);
        assertEquals(
                uacked.longValue(),
                unackedCount - 1,
                "The messages that were polled should be unacked still");

        Long otherUacked = details.get(otherQueueName).get("a").get("uacked");
        assertNotNull(otherUacked);
        assertEquals(
                otherUacked.longValue(), count, "Other queue should have all unacked messages");

        Long size = queueDAO.queuesDetail().get(queueName);
        assertNotNull(size);
        assertEquals(size.longValue(), count - unackedCount);
    }
}
