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
package com.netflix.conductor.client.grpc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.grpc.EventServiceGrpc;
import com.netflix.conductor.grpc.EventServicePb;
import com.netflix.conductor.grpc.ProtoMapper;
import com.netflix.conductor.proto.EventHandlerPb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(SpringRunner.class)
public class EventClientTest {

    @Mock ProtoMapper mockedProtoMapper;

    @Mock EventServiceGrpc.EventServiceBlockingStub mockedStub;

    EventClient eventClient;

    @BeforeEach
    void init() {
        eventClient = new EventClient("test", 0);
        ReflectionTestUtils.setField(eventClient, "stub", mockedStub);
        ReflectionTestUtils.setField(eventClient, "protoMapper", mockedProtoMapper);
    }

    @Test
    void registerEventHandler() {
        EventHandler eventHandler = mock(EventHandler.class);
        EventHandlerPb.EventHandler eventHandlerPB = mock(EventHandlerPb.EventHandler.class);
        when(mockedProtoMapper.toProto(eventHandler)).thenReturn(eventHandlerPB);

        EventServicePb.AddEventHandlerRequest request =
                EventServicePb.AddEventHandlerRequest.newBuilder()
                        .setHandler(eventHandlerPB)
                        .build();
        eventClient.registerEventHandler(eventHandler);
        verify(mockedStub, times(1)).addEventHandler(request);
    }

    @Test
    void updateEventHandler() {
        EventHandler eventHandler = mock(EventHandler.class);
        EventHandlerPb.EventHandler eventHandlerPB = mock(EventHandlerPb.EventHandler.class);
        when(mockedProtoMapper.toProto(eventHandler)).thenReturn(eventHandlerPB);

        EventServicePb.UpdateEventHandlerRequest request =
                EventServicePb.UpdateEventHandlerRequest.newBuilder()
                        .setHandler(eventHandlerPB)
                        .build();
        eventClient.updateEventHandler(eventHandler);
        verify(mockedStub, times(1)).updateEventHandler(request);
    }

    @Test
    void getEventHandlers() {
        EventHandler eventHandler = mock(EventHandler.class);
        EventHandlerPb.EventHandler eventHandlerPB = mock(EventHandlerPb.EventHandler.class);
        when(mockedProtoMapper.fromProto(eventHandlerPB)).thenReturn(eventHandler);
        EventServicePb.GetEventHandlersForEventRequest request =
                EventServicePb.GetEventHandlersForEventRequest.newBuilder()
                        .setEvent("test")
                        .setActiveOnly(true)
                        .build();
        List<EventHandlerPb.EventHandler> result = new ArrayList<>();
        result.add(eventHandlerPB);
        when(mockedStub.getEventHandlersForEvent(request)).thenReturn(result.iterator());
        Iterator<EventHandler> response = eventClient.getEventHandlers("test", true);
        verify(mockedStub, times(1)).getEventHandlersForEvent(request);
        assertEquals(response.next(), eventHandler);
    }

    @Test
    void unregisterEventHandler() {
        EventClient eventClient = createClientWithManagedChannel();
        EventServicePb.RemoveEventHandlerRequest request =
                EventServicePb.RemoveEventHandlerRequest.newBuilder().setName("test").build();
        eventClient.unregisterEventHandler("test");
        verify(mockedStub, times(1)).removeEventHandler(request);
    }

    @Test
    void unregisterEventHandlerWithManagedChannel() {
        EventServicePb.RemoveEventHandlerRequest request =
                EventServicePb.RemoveEventHandlerRequest.newBuilder().setName("test").build();
        eventClient.unregisterEventHandler("test");
        verify(mockedStub, times(1)).removeEventHandler(request);
    }

    public EventClient createClientWithManagedChannel() {
        EventClient eventClient = new EventClient("test", 0);
        ReflectionTestUtils.setField(eventClient, "stub", mockedStub);
        ReflectionTestUtils.setField(eventClient, "protoMapper", mockedProtoMapper);
        return eventClient;
    }
}
