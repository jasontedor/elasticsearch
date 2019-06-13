/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.threads;

import org.elasticsearch.test.ESTestCase;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public class TransportNodesThreadsActionTests extends ESTestCase {

    private final ThreadMXBean mx = ManagementFactory.getThreadMXBean();

    public void testDeadlockWithLocks() {
        final Object firstLock = new Object();
        final Object secondLock = new Object();
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch firstLatch = new CountDownLatch(1);
        final CountDownLatch secondLatch = new CountDownLatch(1);
        final Thread firstThread = new Thread(
                () -> {
                    awaitQuietly(barrier);
                    synchronized (firstLock) {
                        firstLatch.countDown();
                        awaitQuietly(secondLatch);
                        synchronized (secondLock) {

                        }
                    }
                });
        final Thread secondThread = new Thread(
                () -> {
                    awaitQuietly(barrier);
                    synchronized (secondLock) {
                        secondLatch.countDown();
                        awaitQuietly(firstLatch);
                        synchronized (firstLock) {

                        }
                    }
                });
        firstThread.start();
        secondThread.start();
        final Set<LinkedHashSet<Long>> deadlocks = TransportNodesThreadsAction.findDeadlocks(TransportNodesThreadsAction.threads(mx));
        firstThread.stop();
        secondThread.stop();
    }

    public void testDeadlockWithOwnableSynchronizers() {
        final ReentrantLock firstLock = new ReentrantLock();
        final ReentrantLock secondLock = new ReentrantLock();
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch firstLatch = new CountDownLatch(1);
        final CountDownLatch secondLatch = new CountDownLatch(1);
        final Thread firstThread = new Thread(
                () -> {
                    awaitQuietly(barrier);
                    firstLock.lock();
                    firstLatch.countDown();
                    awaitQuietly(secondLatch);
                    secondLock.lock();
                });
        final Thread secondThread = new Thread(
                () -> {
                    awaitQuietly(barrier);
                    secondLock.lock();
                    secondLatch.countDown();
                    awaitQuietly(firstLatch);
                    firstLock.lock();
                });
        firstThread.start();
        secondThread.start();
        final Set<LinkedHashSet<Long>> deadlocks = TransportNodesThreadsAction.findDeadlocks(TransportNodesThreadsAction.threads(mx));
        firstThread.stop();
        secondThread.stop();
    }

    private static void awaitQuietly(final CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (final BrokenBarrierException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void awaitQuietly(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}