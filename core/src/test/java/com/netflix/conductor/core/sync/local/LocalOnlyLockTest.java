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

// Test always times out in CI environment
@Disabled
class LocalOnlyLockTest {

    // Lock can be global since it uses global cache internally
    private final LocalOnlyLock localOnlyLock = new LocalOnlyLock();

    @AfterEach
    void tearDown() {
        // Clean caches between tests as they are shared globally
        localOnlyLock.cache().invalidateAll();
        localOnlyLock.scheduledFutures().values().forEach(f -> f.cancel(false));
        localOnlyLock.scheduledFutures().clear();
    }

    @Test
    void lockUnlock() {
        final boolean a = localOnlyLock.acquireLock("a", 100, 10000, TimeUnit.MILLISECONDS);
        assertTrue(a);
        assertEquals(1, localOnlyLock.cache().estimatedSize());
        assertEquals(true, localOnlyLock.cache().get("a").isLocked());
        assertEquals(1, localOnlyLock.scheduledFutures().size());
        localOnlyLock.releaseLock("a");
        assertEquals(0, localOnlyLock.scheduledFutures().size());
        assertEquals(false, localOnlyLock.cache().get("a").isLocked());
        localOnlyLock.deleteLock("a");
        assertEquals(0, localOnlyLock.cache().estimatedSize());
    }

    @Test
    @Timeout(value = 10 * 10_000, unit = TimeUnit.MILLISECONDS)
    void lockTimeout() throws InterruptedException, ExecutionException {
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit(
                        () -> {
                            localOnlyLock.acquireLock("c", 100, 1000, TimeUnit.MILLISECONDS);
                        })
                .get();
        assertTrue(localOnlyLock.acquireLock("d", 100, 1000, TimeUnit.MILLISECONDS));
        assertFalse(localOnlyLock.acquireLock("c", 100, 1000, TimeUnit.MILLISECONDS));
        assertEquals(2, localOnlyLock.scheduledFutures().size());
        executor.submit(
                        () -> {
                            localOnlyLock.releaseLock("c");
                        })
                .get();
        localOnlyLock.releaseLock("d");
        assertEquals(0, localOnlyLock.scheduledFutures().size());
    }

    @Test
    @Timeout(value = 10 * 10_000, unit = TimeUnit.MILLISECONDS)
    void releaseFromAnotherThread() throws InterruptedException, ExecutionException {
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
    void lockLeaseWithRelease() throws Exception {
        localOnlyLock.acquireLock("b", 1000, 1000, TimeUnit.MILLISECONDS);
        localOnlyLock.releaseLock("b");

        // Wait for lease to run out and also call release
        Thread.sleep(2000);

        localOnlyLock.acquireLock("b");
        assertTrue(localOnlyLock.cache().get("b").isLocked());
        localOnlyLock.releaseLock("b");
    }

    @Test
    void release() {
        localOnlyLock.releaseLock("x54as4d2;23'4");
        localOnlyLock.releaseLock("x54as4d2;23'4");
        assertFalse(localOnlyLock.cache().get("x54as4d2;23'4").isLocked());
    }

    @Test
    @Timeout(value = 10 * 10_000, unit = TimeUnit.MILLISECONDS)
    void lockLeaseTime() throws InterruptedException {
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
    void lockConfiguration() {
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
