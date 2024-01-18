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

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.netflix.conductor.core.events.EventQueues;

import jakarta.validation.ConstraintViolationException;

import static com.netflix.conductor.TestUtils.getConstraintViolationMessages;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.mock;

@SuppressWarnings("SpringJavaAutowiredMembersInspection")
@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
public class EventServiceTest {

    @TestConfiguration
    static class TestEventConfiguration {

        @Bean
        public EventService eventService() {
            MetadataService metadataService = mock(MetadataService.class);
            EventQueues eventQueues = mock(EventQueues.class);
            return new EventServiceImpl(metadataService, eventQueues);
        }
    }

    @Autowired private EventService eventService;

    @Test
    public void testAddEventHandler() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                eventService.addEventHandler(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("EventHandler cannot be null."));
                throw ex;
            }
            fail("eventService.addEventHandler did not throw ConstraintViolationException !");
        });
    }

    @Test
    public void testUpdateEventHandler() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                eventService.updateEventHandler(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("EventHandler cannot be null."));
                throw ex;
            }
            fail("eventService.updateEventHandler did not throw ConstraintViolationException !");
        });
    }

    @Test
    public void testRemoveEventHandlerStatus() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                eventService.removeEventHandlerStatus(null);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("EventHandler name cannot be null or empty."));
                throw ex;
            }
            fail("eventService.removeEventHandlerStatus did not throw ConstraintViolationException !");
        });
    }

    @Test
    public void testGetEventHandlersForEvent() {
        assertThrows(ConstraintViolationException.class, () -> {
            try {
                eventService.getEventHandlersForEvent(null, false);
            } catch (ConstraintViolationException ex) {
                assertEquals(1, ex.getConstraintViolations().size());
                Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
                assertTrue(messages.contains("Event cannot be null or empty."));
                throw ex;
            }
            fail("eventService.getEventHandlersForEvent did not throw ConstraintViolationException !");
        });
    }
}
