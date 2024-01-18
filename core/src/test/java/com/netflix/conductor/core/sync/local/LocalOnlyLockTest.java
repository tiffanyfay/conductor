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
package com.netflix.conductor.core.sync.local;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
// Test always times out in CI environment
public class LocalOnlyLockTest {

    // Lock can be global since it uses global cache internally
    private final LocalOnlyLock localOnlyLock = new LocalOnlyLock();

    @AfterEach
    public void tearDown() {
        // Clean caches between tests as they are shared globally
        localOnlyLock.cache().invalidateAll();
        localOnlyLock.scheduledFutures().values().forEach(f -> f.cancel(false));
        localOnlyLock.scheduledFutures().clear();
    }

    @Test
    public void testLockUnlock() {
        final boolean a = localOnlyLock.acquireLock("a", 100, 10000, TimeUnit.MILLISECONDS);
        assertTrue(a);
        assertEquals(localOnlyLock.cache().estimatedSize(), 1);
        assertEquals(localOnlyLock.cache().get("a").isLocked(), true);
        assertEquals(localOnlyLock.scheduledFutures().size(), 1);
        localOnlyLock.releaseLock("a");
        assertEquals(localOnlyLock.scheduledFutures().size(), 0);
        assertEquals(localOnlyLock.cache().get("a").isLocked(), false);
        localOnlyLock.deleteLock("a");
        assertEquals(localOnlyLock.cache().estimatedSize(), 0);
    }

    @Test
    @Timeout(value = 10 * 10_000, unit = TimeUnit.MILLISECONDS)
    public void testLockTimeout() throws InterruptedException, ExecutionException {
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit(
                        () -> {
                            localOnlyLock.acquireLock("c", 100, 1000, TimeUnit.MILLISECONDS);
                        })
                .get();
        assertTrue(localOnlyLock.acquireLock("d", 100, 1000, TimeUnit.MILLISECONDS));
        assertFalse(localOnlyLock.acquireLock("c", 100, 1000, TimeUnit.MILLISECONDS));
        assertEquals(localOnlyLock.scheduledFutures().size(), 2);
        executor.submit(
                        () -> {
                            localOnlyLock.releaseLock("c");
                        })
                .get();
        localOnlyLock.releaseLock("d");
        assertEquals(localOnlyLock.scheduledFutures().size(), 0);
    }

    @Test
    @Timeout(value = 10 * 10_000, unit = TimeUnit.MILLISECONDS)
    public void testReleaseFromAnotherThread() throws InterruptedException, ExecutionException {
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit(
                        () -> {
                            localOnlyLock.acquireLock("c", 100, 10000, TimeUnit.MILLISECONDS);
                        })
                .get();
        try {
            localOnlyLock.releaseLock("c");
        } catch (IllegalMonitorStateException e) {
            // expected
            localOnlyLock.deleteLock("c");
            return;
        } finally {
            executor.submit(
                            () -> {
                                localOnlyLock.releaseLock("c");
                            })
                    .get();
        }

        fail();
    }

    @Test
    @Timeout(value = 10 * 10_000, unit = TimeUnit.MILLISECONDS)
    public void testLockLeaseWithRelease() throws Exception {
        localOnlyLock.acquireLock("b", 1000, 1000, TimeUnit.MILLISECONDS);
        localOnlyLock.releaseLock("b");

        // Wait for lease to run out and also call release
        Thread.sleep(2000);

        localOnlyLock.acquireLock("b");
        assertEquals(true, localOnlyLock.cache().get("b").isLocked());
        localOnlyLock.releaseLock("b");
    }

    @Test
    public void testRelease() {
        localOnlyLock.releaseLock("x54as4d2;23'4");
        localOnlyLock.releaseLock("x54as4d2;23'4");
        assertEquals(false, localOnlyLock.cache().get("x54as4d2;23'4").isLocked());
    }

    @Test
    @Timeout(value = 10 * 10_000, unit = TimeUnit.MILLISECONDS)
    public void testLockLeaseTime() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            final Thread thread =
                    new Thread(
                            () -> {
                                localOnlyLock.acquireLock("a", 1000, 100, TimeUnit.MILLISECONDS);
                            });
            thread.start();
            thread.join();
        }
        localOnlyLock.acquireLock("a");
        assertTrue(localOnlyLock.cache().get("a").isLocked());
        localOnlyLock.releaseLock("a");
        localOnlyLock.deleteLock("a");
    }

    @Test
    public void testLockConfiguration() {
        new ApplicationContextRunner()
                .withPropertyValues("conductor.workflow-execution-lock.type=local_only")
                .withUserConfiguration(LocalOnlyLockConfiguration.class)
                .run(
                        context -> {
                            LocalOnlyLock lock = context.getBean(LocalOnlyLock.class);
                            assertNotNull(lock);
                        });
    }
}
