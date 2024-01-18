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
package com.netflix.conductor.redis.lock;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.*;

import com.netflix.conductor.redislock.config.RedisLockProperties;
import com.netflix.conductor.redislock.config.RedisLockProperties.REDIS_SERVER_TYPE;
import com.netflix.conductor.redislock.lock.RedisLock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisLockTest {

    private static RedisLock redisLock;
    private static Config config;
    private static RedissonClient redisson;

    static GenericContainer redis =
            new GenericContainer("redis:5.0.3-alpine").withExposedPorts(6379);

    @BeforeAll
    static void setUp() throws Exception {
        redis.start();
        int port = redis.getFirstMappedPort();
        String host = redis.getHost();
        String testServerAddress = "redis://" + host + ":" + port;

        RedisLockProperties properties = mock(RedisLockProperties.class);
        when(properties.getServerType()).thenReturn(REDIS_SERVER_TYPE.SINGLE);
        when(properties.getServerAddress()).thenReturn(testServerAddress);
        when(properties.getServerMasterName()).thenReturn("master");
        when(properties.getNamespace()).thenReturn("");
        when(properties.isIgnoreLockingExceptions()).thenReturn(false);

        Config redissonConfig = new Config();
        redissonConfig.useSingleServer().setAddress(testServerAddress).setTimeout(10000);
        redisLock = new RedisLock((Redisson) Redisson.create(redissonConfig), properties);

        // Create another instance of redisson for tests.
        RedisLockTest.config = new Config();
        RedisLockTest.config.useSingleServer().setAddress(testServerAddress).setTimeout(10000);
        redisson = Redisson.create(RedisLockTest.config);
    }

    @AfterAll
    static void tearDown() {
        redis.stop();
    }

    @Test
    void locking() {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";
        assertTrue(redisLock.acquireLock(lockId, 1000, 1000, TimeUnit.MILLISECONDS));
    }

    @Test
    void lockExpiration() throws InterruptedException {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";
        boolean isLocked = redisLock.acquireLock(lockId, 1000, 1000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        Thread.sleep(2000);

        RLock lock = redisson.getLock(lockId);
        assertFalse(lock.isLocked());
    }

    @Test
    void lockReentry() throws InterruptedException {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";
        boolean isLocked = redisLock.acquireLock(lockId, 1000, 60000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        Thread.sleep(1000);

        // get the lock back
        isLocked = redisLock.acquireLock(lockId, 1000, 1000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        RLock lock = redisson.getLock(lockId);
        assertTrue(isLocked);
    }

    @Test
    void releaseLock() {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";

        boolean isLocked = redisLock.acquireLock(lockId, 1000, 10000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        redisLock.releaseLock(lockId);

        RLock lock = redisson.getLock(lockId);
        assertFalse(lock.isLocked());
    }

    @Test
    void lockReleaseAndAcquire() throws InterruptedException {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";

        boolean isLocked = redisLock.acquireLock(lockId, 1000, 10000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        redisLock.releaseLock(lockId);

        Worker worker1 = new Worker(redisLock, lockId);

        worker1.start();
        worker1.join();

        assertTrue(worker1.isLocked);
    }

    @Test
    void lockingDuplicateThreads() throws InterruptedException {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";

        Worker worker1 = new Worker(redisLock, lockId);
        Worker worker2 = new Worker(redisLock, lockId);

        worker1.start();
        worker2.start();

        worker1.join();
        worker2.join();

        // Ensure only one of them had got the lock.
        assertFalse(worker1.isLocked && worker2.isLocked);
        assertTrue(worker1.isLocked || worker2.isLocked);
    }

    @Test
    void duplicateLockAcquireFailure() throws InterruptedException {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";
        Worker worker1 = new Worker(redisLock, lockId, 100L, 60000L);

        worker1.start();
        worker1.join();

        boolean isLocked = redisLock.acquireLock(lockId, 500L, 1000L, TimeUnit.MILLISECONDS);

        // Ensure only one of them had got the lock.
        assertFalse(isLocked);
        assertTrue(worker1.isLocked);
    }

    @Test
    void reacquireLostKey() {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";

        boolean isLocked = redisLock.acquireLock(lockId, 1000, 10000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        // Delete key from the cluster to reacquire
        // Simulating the case when cluster goes down and possibly loses some keys.
        redisson.getKeys().flushall();

        isLocked = redisLock.acquireLock(lockId, 100, 10000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);
    }

    @Test
    void releaseLockTwice() {
        redisson.getKeys().flushall();
        String lockId = "abcd-1234";

        boolean isLocked = redisLock.acquireLock(lockId, 1000, 10000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        redisLock.releaseLock(lockId);
        redisLock.releaseLock(lockId);
    }

    private static class Worker extends Thread {

        private final RedisLock lock;
        private final String lockID;
        boolean isLocked;
        private Long timeToTry = 50L;
        private Long leaseTime = 1000L;

        Worker(RedisLock lock, String lockID) {
            super("TestWorker-" + lockID);
            this.lock = lock;
            this.lockID = lockID;
        }

        Worker(RedisLock lock, String lockID, Long timeToTry, Long leaseTime) {
            super("TestWorker-" + lockID);
            this.lock = lock;
            this.lockID = lockID;
            this.timeToTry = timeToTry;
            this.leaseTime = leaseTime;
        }

        @Override
        public void run() {
            isLocked = lock.acquireLock(lockID, timeToTry, leaseTime, TimeUnit.MILLISECONDS);
        }
    }
}
