/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class ShardFollowNodeTaskRandomTests extends ESTestCase {

    public void testSingleReaderWriter() throws Exception {
        TestRun testRun = createTestRun(randomNonNegativeLong(), randomNonNegativeLong(), randomIntBetween(1, 2048));
        ShardFollowNodeTask task = createShardFollowTask(1, testRun);
        startAndAssertAndStopTask(task, testRun);
    }

    public void testMultipleReaderWriter() throws Exception {
        int concurrency = randomIntBetween(2, 8);
        TestRun testRun = createTestRun(0, 0, 1024);
        ShardFollowNodeTask task = createShardFollowTask(concurrency, testRun);
        startAndAssertAndStopTask(task, testRun);
    }

    private void startAndAssertAndStopTask(ShardFollowNodeTask task, TestRun testRun) throws Exception {
        task.start(testRun.startSeqNo - 1, testRun.startSeqNo - 1);
        assertBusy(() -> {
            ShardFollowNodeTask.Status status = task.getStatus();
            assertThat(status.leaderGlobalCheckpoint(), equalTo(testRun.finalExpectedGlobalCheckpoint));
            assertThat(status.followerGlobalCheckpoint(), equalTo(testRun.finalExpectedGlobalCheckpoint));
            assertThat(status.indexMetadataVersion(), equalTo(testRun.finalIndexMetaDataVerion));
        });

        task.markAsCompleted();
        assertBusy(() -> {
            ShardFollowNodeTask.Status status = task.getStatus();
            assertThat(status.numberOfConcurrentReads(), equalTo(0));
            assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        });
    }

    private ShardFollowNodeTask createShardFollowTask(int concurrency, TestRun testRun) {
        AtomicBoolean stopped = new AtomicBoolean(false);
        ShardFollowTask params = new ShardFollowTask(null, new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0), testRun.maxOperationCount, concurrency,
            ShardFollowNodeTask.DEFAULT_MAX_BATCH_SIZE_IN_BYTES, concurrency, 10240,
            TimeValue.timeValueMillis(10), TimeValue.timeValueMillis(10), Collections.emptyMap());

        ThreadPool threadPool = new TestThreadPool(getClass().getSimpleName());
        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> {
            assert delay.millis() < 100 : "The delay should be kept to a minimum, so that this test does not take to long to run";
            threadPool.schedule(delay, ThreadPool.Names.GENERIC, task);
        };
        List<Translog.Operation> receivedOperations = Collections.synchronizedList(new ArrayList<>());
        LocalCheckpointTracker tracker = new LocalCheckpointTracker(testRun.startSeqNo - 1, testRun.startSeqNo - 1);
        return new ShardFollowNodeTask(1L, "type", ShardFollowTask.NAME, "description", null, Collections.emptyMap(), params, scheduler) {

            private volatile long indexMetadataVersion = 0L;
            private final Map<Long, Integer> fromToSlot = new HashMap<>();

            @Override
            protected void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler) {
                handler.accept(indexMetadataVersion);
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(List<Translog.Operation> operations, LongConsumer handler,
                                                               Consumer<Exception> errorHandler) {
                for(Translog.Operation op : operations) {
                    tracker.markSeqNoAsCompleted(op.seqNo());
                }
                receivedOperations.addAll(operations);

                // Emulate network thread and avoid SO:
                threadPool.generic().execute(() -> handler.accept(tracker.getCheckpoint()));
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {

                // Emulate network thread and avoid SO:
                Runnable task = () -> {
                    List<TestResponse> items = testRun.responses.get(from);
                    if (items != null) {
                        final TestResponse testResponse;
                        synchronized (fromToSlot) {
                            int slot;
                            if (fromToSlot.get(from) == null) {
                                slot = fromToSlot.getOrDefault(from, 0);
                                fromToSlot.put(from, slot);
                            } else {
                                slot = fromToSlot.get(from);
                            }
                            testResponse = items.get(slot);
                            fromToSlot.put(from, ++slot);
                            // if too many invocations occur with the same from then AOBE occurs, this ok and then something is wrong.
                        }
                        indexMetadataVersion = testResponse.indexMetadataVersion;
                        if (testResponse.exception != null) {
                            errorHandler.accept(testResponse.exception);
                        } else {
                            handler.accept(testResponse.response);
                        }
                    } else {
                        assert from >= testRun.finalExpectedGlobalCheckpoint;
                        handler.accept(new ShardChangesAction.Response(0L, tracker.getCheckpoint(), new Translog.Operation[0]));
                    }
                };
                threadPool.generic().execute(task);
            }

            @Override
            protected boolean isStopped() {
                return stopped.get();
            }

            @Override
            public void markAsCompleted() {
                stopped.set(true);
                tearDown();
            }

            @Override
            public void markAsFailed(Exception e) {
                stopped.set(true);
                tearDown();
            }

            private void tearDown() {
                threadPool.shutdown();
                List<Translog.Operation> expectedOperations = testRun.responses.values().stream()
                    .flatMap(List::stream)
                    .map(testResponse -> testResponse.response)
                    .filter(Objects::nonNull)
                    .flatMap(response -> Arrays.stream(response.getOperations()))
                    .sorted(Comparator.comparingLong(Translog.Operation::seqNo))
                    .collect(Collectors.toList());
                assertThat(receivedOperations.size(), equalTo(expectedOperations.size()));
                receivedOperations.sort(Comparator.comparingLong(Translog.Operation::seqNo));
                for (int i = 0; i < receivedOperations.size(); i++) {
                    Translog.Operation actual = receivedOperations.get(i);
                    Translog.Operation expected = expectedOperations.get(i);
                    assertThat(actual, equalTo(expected));
                }
            }
        };
    }

    private static TestRun createTestRun(long startSeqNo, long startIndexMetadataVersion, int maxOperationCount) {
        long prevGlobalCheckpoint = startSeqNo;
        long indexMetaDataVersion = startIndexMetadataVersion;
        int numResponses = randomIntBetween(16, 256);
        Map<Long, List<TestResponse>> responses = new HashMap<>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            long nextGlobalCheckPoint = prevGlobalCheckpoint + maxOperationCount;
            if (sometimes()) {
                indexMetaDataVersion++;
            }

            if (sometimes()) {
                List<TestResponse> item = new ArrayList<>();
                // Sometimes add a random retryable error
                if (sometimes()) {
                    Exception error = new UnavailableShardsException(new ShardId("test", "test", 0), "");
                    item.add(new TestResponse(error, indexMetaDataVersion, null));
                }
                List<Translog.Operation> ops = new ArrayList<>();
                for (long seqNo = prevGlobalCheckpoint; seqNo <= nextGlobalCheckPoint; seqNo++) {
                    String id = UUIDs.randomBase64UUID();
                    byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
                    ops.add(new Translog.Index("doc", id, seqNo, 0, source));
                }
                item.add(new TestResponse(null, indexMetaDataVersion,
                    new ShardChangesAction.Response(indexMetaDataVersion, nextGlobalCheckPoint, ops.toArray(EMPTY))));
                responses.put(prevGlobalCheckpoint, item);
            } else {
                // Simulates a leader shard copy not having all the operations the shard follow task thinks it has by
                // splitting up a response into multiple responses AND simulates maxBatchSizeInBytes limit being reached:
                long toSeqNo;
                for (long fromSeqNo = prevGlobalCheckpoint; fromSeqNo <= nextGlobalCheckPoint; fromSeqNo = toSeqNo + 1) {
                    toSeqNo = randomLongBetween(fromSeqNo, nextGlobalCheckPoint);
                    List<TestResponse> item = new ArrayList<>();
                    // Sometimes add a random retryable error
                    if (sometimes()) {
                        Exception error = new UnavailableShardsException(new ShardId("test", "test", 0), "");
                        item.add(new TestResponse(error, indexMetaDataVersion, null));
                    }
                    // Sometimes add an empty shard changes response to also simulate a leader shard lagging behind
                    if (sometimes()) {
                        ShardChangesAction.Response response =
                            new ShardChangesAction.Response(indexMetaDataVersion, prevGlobalCheckpoint, EMPTY);
                        item.add(new TestResponse(null, indexMetaDataVersion, response));
                    }
                    List<Translog.Operation> ops = new ArrayList<>();
                    for (long seqNo = fromSeqNo; seqNo <= toSeqNo; seqNo++) {
                        String id = UUIDs.randomBase64UUID();
                        byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
                        ops.add(new Translog.Index("doc", id, seqNo, 0, source));
                    }
                    // Report toSeqNo to simulate maxBatchSizeInBytes limit being met or last op to simulate a shard lagging behind:
                    long localLeaderGCP = randomBoolean() ? ops.get(ops.size() - 1).seqNo() : toSeqNo;
                    ShardChangesAction.Response response = new ShardChangesAction.Response(indexMetaDataVersion,
                        localLeaderGCP, ops.toArray(EMPTY));
                    item.add(new TestResponse(null, indexMetaDataVersion, response));
                    responses.put(fromSeqNo, Collections.unmodifiableList(item));
                }
            }
            prevGlobalCheckpoint = nextGlobalCheckPoint + 1;
        }
        return new TestRun(maxOperationCount, startSeqNo, startIndexMetadataVersion, indexMetaDataVersion,
            prevGlobalCheckpoint - 1, responses);
    }

    // Instead of rarely(), which returns true very rarely especially not running in nightly mode or a multiplier have not been set
    private static boolean sometimes() {
        return randomIntBetween(0, 10) == 5;
    }

    private static class TestRun {

        final int maxOperationCount;
        final long startSeqNo;
        final long startIndexMetadataVersion;

        final long finalIndexMetaDataVerion;
        final long finalExpectedGlobalCheckpoint;
        final Map<Long, List<TestResponse>> responses;

        private TestRun(int maxOperationCount, long startSeqNo, long startIndexMetadataVersion, long finalIndexMetaDataVerion,
                        long finalExpectedGlobalCheckpoint, Map<Long, List<TestResponse>> responses) {
            this.maxOperationCount = maxOperationCount;
            this.startSeqNo = startSeqNo;
            this.startIndexMetadataVersion = startIndexMetadataVersion;
            this.finalIndexMetaDataVerion = finalIndexMetaDataVerion;
            this.finalExpectedGlobalCheckpoint = finalExpectedGlobalCheckpoint;
            this.responses = Collections.unmodifiableMap(responses);
        }
    }

    private static class TestResponse {

        final Exception exception;
        final long indexMetadataVersion;
        final ShardChangesAction.Response response;

        private TestResponse(Exception exception, long indexMetadataVersion, ShardChangesAction.Response response) {
            this.exception = exception;
            this.indexMetadataVersion = indexMetadataVersion;
            this.response = response;
        }
    }

    private static final Translog.Operation[] EMPTY = new Translog.Operation[0];

}
