/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * The node task that fetch the write operations from a leader shard and
 * persists these ops in the follower shard.
 */
public abstract class ShardFollowNodeTask extends AllocatedPersistentTask {

    public static final int DEFAULT_MAX_BATCH_OPERATION_COUNT = 1024;
    public static final int DEFAULT_MAX_CONCURRENT_READ_BATCHES = 1;
    public static final int DEFAULT_MAX_CONCURRENT_WRITE_BATCHES = 1;
    public static final int DEFAULT_MAX_WRITE_BUFFER_SIZE = 10240;
    public static final long DEFAULT_MAX_BATCH_SIZE_IN_BYTES = Long.MAX_VALUE;
    private static final int RETRY_LIMIT = 10;
    public static final TimeValue DEFAULT_RETRY_TIMEOUT = new TimeValue(500);
    public static final TimeValue DEFAULT_IDLE_SHARD_RETRY_DELAY = TimeValue.timeValueSeconds(10);

    private static final Logger LOGGER = Loggers.getLogger(ShardFollowNodeTask.class);

    private final ShardFollowTask params;
    private final TimeValue retryTimeout;
    private final TimeValue idleShardChangesRequestDelay;
    private final BiConsumer<TimeValue, Runnable> scheduler;
    private final LongSupplier relativeTimeProvider;

    private final ReleasableLock readLock;
    private final ReleasableLock writeLock;

    {
        final ReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = new ReleasableLock(lock.readLock());
        this.writeLock = new ReleasableLock(lock.writeLock());
    }

    private volatile long leaderGlobalCheckpoint;
    private volatile long leaderMaxSeqNo;
    private volatile long lastRequestedSeqNo;
    private volatile long followerGlobalCheckpoint = 0;
    private volatile long followerMaxSeqNo = 0;
    private volatile int numConcurrentReads = 0;
    private volatile int numConcurrentWrites = 0;
    private volatile long currentIndexMetadataVersion = 0;
    private volatile long totalFetchTimeNanos = 0;
    private volatile long numberOfSuccessfulFetches = 0;
    private volatile long numberOfFailedFetches = 0;
    private volatile long operationsReceived = 0;
    private volatile long totalTransferredBytes = 0;
    private volatile long totalIndexTimeNanos = 0;
    private volatile long numberOfSuccessfulBulkOperations = 0;
    private volatile long numberOfFailedBulkOperations = 0;
    private volatile long numberOfOperationsIndexed = 0;
    private final Queue<Translog.Operation> buffer = new PriorityQueue<>(Comparator.comparing(Translog.Operation::seqNo));

    ShardFollowNodeTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers,
                        ShardFollowTask params, BiConsumer<TimeValue, Runnable> scheduler, final LongSupplier relativeTimeProvider) {
        super(id, type, action, description, parentTask, headers);
        this.params = params;
        this.scheduler = scheduler;
        this.relativeTimeProvider = relativeTimeProvider;
        this.retryTimeout = params.getRetryTimeout();
        this.idleShardChangesRequestDelay = params.getIdleShardRetryDelay();
    }

    void start(
            final long leaderGlobalCheckpoint,
            final long leaderMaxSeqNo,
            final long followerGlobalCheckpoint,
            final long followerMaxSeqNo) {
        this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
        this.leaderMaxSeqNo = leaderMaxSeqNo;
        this.followerGlobalCheckpoint = followerGlobalCheckpoint;
        this.followerMaxSeqNo = followerMaxSeqNo;
        this.lastRequestedSeqNo = followerGlobalCheckpoint;

        // Forcefully updates follower mapping, this gets us the leader imd version and
        // makes sure that leader and follower mapping are identical.
        updateMapping(imdVersion -> {
            currentIndexMetadataVersion = imdVersion;
            LOGGER.info("{} Started to follow leader shard {}, followGlobalCheckPoint={}, indexMetaDataVersion={}",
                params.getFollowShardId(), params.getLeaderShardId(), followerGlobalCheckpoint, imdVersion);
            coordinateReads();
        });
    }

    synchronized void coordinateReads() {
        if (isStopped()) {
            LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            return;
        }

        LOGGER.trace("{} coordinate reads, lastRequestedSeqNo={}, leaderGlobalCheckpoint={}",
            params.getFollowShardId(), lastRequestedSeqNo, leaderGlobalCheckpoint);
        final int maxBatchOperationCount = params.getMaxBatchOperationCount();
        while (hasReadBudget() && lastRequestedSeqNo < leaderGlobalCheckpoint) {
            numConcurrentReads++;
            long from = lastRequestedSeqNo + 1;
            // -1 is needed, because maxRequiredSeqno is inclusive
            long maxRequiredSeqno = Math.min(leaderGlobalCheckpoint, (from + maxBatchOperationCount) - 1);
            LOGGER.trace("{}[{}] read [{}/{}]", params.getFollowShardId(), numConcurrentReads, maxRequiredSeqno, maxBatchOperationCount);
            sendShardChangesRequest(from, maxBatchOperationCount, maxRequiredSeqno);
            lastRequestedSeqNo = maxRequiredSeqno;
        }

        if (numConcurrentReads == 0 && hasReadBudget()) {
            assert lastRequestedSeqNo == leaderGlobalCheckpoint;
            // We sneak peek if there is any thing new in the leader.
            // If there is we will happily accept
            numConcurrentReads++;
            long from = lastRequestedSeqNo + 1;
            LOGGER.trace("{}[{}] peek read [{}]", params.getFollowShardId(), numConcurrentReads, from);
            sendShardChangesRequest(from, maxBatchOperationCount, lastRequestedSeqNo);
        }
    }

    private boolean hasReadBudget() {
        assert Thread.holdsLock(this);
        if (numConcurrentReads >= params.getMaxConcurrentReadBatches()) {
            LOGGER.trace("{} no new reads, maximum number of concurrent reads have been reached [{}]",
                params.getFollowShardId(), numConcurrentReads);
            return false;
        }
        if (buffer.size() > params.getMaxWriteBufferSize()) {
            LOGGER.trace("{} no new reads, buffer limit has been reached [{}]", params.getFollowShardId(), buffer.size());
            return false;
        }
        return true;
    }

    private synchronized void coordinateWrites() {
        if (isStopped()) {
            LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            return;
        }

        while (hasWriteBudget() && buffer.isEmpty() == false) {
            long sumEstimatedSize = 0L;
            int length = Math.min(params.getMaxBatchOperationCount(), buffer.size());
            List<Translog.Operation> ops = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                Translog.Operation op = buffer.remove();
                ops.add(op);
                sumEstimatedSize += op.estimateSize();
                if (sumEstimatedSize > params.getMaxBatchSizeInBytes()) {
                    break;
                }
            }
            numConcurrentWrites++;
            LOGGER.trace("{}[{}] write [{}/{}] [{}]", params.getFollowShardId(), numConcurrentWrites, ops.get(0).seqNo(),
                ops.get(ops.size() - 1).seqNo(), ops.size());
            sendBulkShardOperationsRequest(ops);
        }
    }

    private boolean hasWriteBudget() {
        assert Thread.holdsLock(this);
        if (numConcurrentWrites >= params.getMaxConcurrentWriteBatches()) {
            LOGGER.trace("{} maximum number of concurrent writes have been reached [{}]",
                params.getFollowShardId(), numConcurrentWrites);
            return false;
        }
        return true;
    }

    private void sendShardChangesRequest(long from, int maxOperationCount, long maxRequiredSeqNo) {
        sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, new AtomicInteger(0));
    }

    private void sendShardChangesRequest(long from, int maxOperationCount, long maxRequiredSeqNo, AtomicInteger retryCounter) {
        final long startTime = relativeTimeProvider.getAsLong();
        innerSendShardChangesRequest(from, maxOperationCount,
                response -> {
                    try (ReleasableLock ignored = writeLock.acquire()) {
                        // noinspection NonAtomicOperationOnVolatileField
                        totalFetchTimeNanos += relativeTimeProvider.getAsLong() - startTime;
                        // noinspection NonAtomicOperationOnVolatileField
                        numberOfSuccessfulFetches++;
                        // noinspection NonAtomicOperationOnVolatileField
                        operationsReceived += response.getOperations().length;
                        // noinspection NonAtomicOperationOnVolatileField
                        totalTransferredBytes += Arrays.stream(response.getOperations()).mapToLong(Translog.Operation::estimateSize).sum();
                    }
                    handleReadResponse(from, maxRequiredSeqNo, response);
                },
                e -> {
                    try (ReleasableLock ignored = writeLock.acquire()) {
                        // noinspection NonAtomicOperationOnVolatileField
                        totalFetchTimeNanos += relativeTimeProvider.getAsLong() - startTime;
                        // noinspection NonAtomicOperationOnVolatileField
                        numberOfFailedFetches++;
                    }
                    handleFailure(e, retryCounter, () -> sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, retryCounter));
                });
    }

    void handleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        maybeUpdateMapping(response.getIndexMetadataVersion(), () -> innerHandleReadResponse(from, maxRequiredSeqNo, response));
    }

    synchronized void innerHandleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        try (ReleasableLock ignored = writeLock.acquire()) {
            leaderGlobalCheckpoint = Math.max(leaderGlobalCheckpoint, response.getGlobalCheckpoint());
            leaderMaxSeqNo = Math.max(leaderMaxSeqNo, response.getMaxSeqNo());
        }
        final long newFromSeqNo;
        if (response.getOperations().length == 0) {
            newFromSeqNo = from;
        } else {
            assert response.getOperations()[0].seqNo() == from :
                "first operation is not what we asked for. From is [" + from + "], got " + response.getOperations()[0];
            buffer.addAll(Arrays.asList(response.getOperations()));
            final long maxSeqNo = response.getOperations()[response.getOperations().length - 1].seqNo();
            assert maxSeqNo ==
                Arrays.stream(response.getOperations()).mapToLong(Translog.Operation::seqNo).max().getAsLong();
            newFromSeqNo = maxSeqNo + 1;
            // update last requested seq no as we may have gotten more than we asked for and we don't want to ask it again.
            lastRequestedSeqNo = Math.max(lastRequestedSeqNo, maxSeqNo);
            assert lastRequestedSeqNo <= leaderGlobalCheckpoint :  "lastRequestedSeqNo [" + lastRequestedSeqNo +
                "] is larger than the global checkpoint [" + leaderGlobalCheckpoint + "]";
            coordinateWrites();
        }
        if (newFromSeqNo <= maxRequiredSeqNo && isStopped() == false) {
            int newSize = Math.toIntExact(maxRequiredSeqNo - newFromSeqNo + 1);
            LOGGER.trace("{} received [{}] ops, still missing [{}/{}], continuing to read...",
                params.getFollowShardId(), response.getOperations().length, newFromSeqNo, maxRequiredSeqNo);
            sendShardChangesRequest(newFromSeqNo, newSize, maxRequiredSeqNo);
        } else {
            // read is completed, decrement
            numConcurrentReads--;
            if (response.getOperations().length == 0 && leaderGlobalCheckpoint == lastRequestedSeqNo)  {
                // we got nothing and we have no reason to believe asking again well get us more, treat shard as idle and delay
                // future requests
                LOGGER.trace("{} received no ops and no known ops to fetch, scheduling to coordinate reads",
                    params.getFollowShardId());
                scheduler.accept(idleShardChangesRequestDelay, this::coordinateReads);
            } else {
                coordinateReads();
            }
        }
    }

    private void sendBulkShardOperationsRequest(List<Translog.Operation> operations) {
        sendBulkShardOperationsRequest(operations, new AtomicInteger(0));
    }

    private void sendBulkShardOperationsRequest(List<Translog.Operation> operations, AtomicInteger retryCounter) {
        final long startTime = relativeTimeProvider.getAsLong();
        innerSendBulkShardOperationsRequest(operations,
                response -> {
                    try (ReleasableLock ignored = writeLock.acquire()) {
                        // noinspection NonAtomicOperationOnVolatileField
                        totalIndexTimeNanos += relativeTimeProvider.getAsLong() - startTime;
                        // noinspection NonAtomicOperationOnVolatileField
                        numberOfSuccessfulBulkOperations++;
                        // noinspection NonAtomicOperationOnVolatileField
                        numberOfOperationsIndexed += operations.size();
                        handleWriteResponse(response);
                    }
                },
                e -> {
                    try (ReleasableLock ignored = writeLock.acquire()) {
                        // noinspection NonAtomicOperationOnVolatileField
                        totalIndexTimeNanos += relativeTimeProvider.getAsLong() - startTime;
                        // noinspection NonAtomicOperationOnVolatileField
                        numberOfFailedBulkOperations++;
                    }
                    handleFailure(e, retryCounter, () -> sendBulkShardOperationsRequest(operations, retryCounter));
                }
        );
    }

    private synchronized void handleWriteResponse(final BulkShardOperationsResponse response) {
        try (ReleasableLock ignored = writeLock.acquire()) {
            this.followerGlobalCheckpoint = Math.max(this.followerGlobalCheckpoint, response.getGlobalCheckpoint());
            this.followerMaxSeqNo = Math.max(this.followerMaxSeqNo, response.getMaxSeqNo());
        }
        numConcurrentWrites--;
        assert numConcurrentWrites >= 0;
        coordinateWrites();

        // In case that buffer has more ops than is allowed then reads may all have been stopped,
        // this invocation makes sure that we start a read when there is budget in case no reads are being performed.
        coordinateReads();
    }

    private synchronized void maybeUpdateMapping(Long minimumRequiredIndexMetadataVersion, Runnable task) {
        if (currentIndexMetadataVersion >= minimumRequiredIndexMetadataVersion) {
            LOGGER.trace("{} index metadata version [{}] is higher or equal than minimum required index metadata version [{}]",
                params.getFollowShardId(), currentIndexMetadataVersion, minimumRequiredIndexMetadataVersion);
            task.run();
        } else {
            LOGGER.trace("{} updating mapping, index metadata version [{}] is lower than minimum required index metadata version [{}]",
                params.getFollowShardId(), currentIndexMetadataVersion, minimumRequiredIndexMetadataVersion);
            updateMapping(imdVersion -> {
                currentIndexMetadataVersion = imdVersion;
                task.run();
            });
        }
    }

    private void updateMapping(LongConsumer handler) {
        updateMapping(handler, new AtomicInteger(0));
    }

    private void updateMapping(LongConsumer handler, AtomicInteger retryCounter) {
        innerUpdateMapping(handler, e -> handleFailure(e, retryCounter, () -> updateMapping(handler, retryCounter)));
    }

    private void handleFailure(Exception e, AtomicInteger retryCounter, Runnable task) {
        assert e != null;
        if (shouldRetry(e)) {
            if (isStopped() == false && retryCounter.incrementAndGet() <= RETRY_LIMIT) {
                LOGGER.debug(new ParameterizedMessage("{} error during follow shard task, retrying...", params.getFollowShardId()), e);
                scheduler.accept(retryTimeout, task);
            } else {
                markAsFailed(new ElasticsearchException("retrying failed [" + retryCounter.get() +
                    "] times, aborting...", e));
            }
        } else {
            markAsFailed(e);
        }
    }

    private boolean shouldRetry(Exception e) {
        return NetworkExceptionHelper.isConnectException(e) ||
            NetworkExceptionHelper.isCloseConnectionException(e) ||
            TransportActions.isShardNotAvailableException(e);
    }

    // These methods are protected for testing purposes:
    protected abstract void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerSendBulkShardOperationsRequest(
            final List<Translog.Operation> operations,
            final Consumer<BulkShardOperationsResponse> handler,
            final Consumer<Exception> errorHandler);

    protected abstract void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                         Consumer<Exception> errorHandler);

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }

    protected boolean isStopped() {
        return isCancelled() || isCompleted();
    }

    public ShardId getFollowShardId() {
        return params.getFollowShardId();
    }

    @Override
    public Status getStatus() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return new Status(
                    leaderGlobalCheckpoint,
                    leaderMaxSeqNo,
                    followerGlobalCheckpoint,
                    followerMaxSeqNo,
                    lastRequestedSeqNo,
                    numConcurrentReads,
                    numConcurrentWrites,
                    currentIndexMetadataVersion,
                    totalFetchTimeNanos,
                    numberOfSuccessfulFetches,
                    numberOfFailedFetches,
                    operationsReceived,
                    totalTransferredBytes,
                    totalIndexTimeNanos,
                    numberOfSuccessfulBulkOperations,
                    numberOfFailedBulkOperations,
                    numberOfOperationsIndexed);
        }
    }

    public static class Status implements Task.Status {

        public static final String NAME = "shard-follow-node-task-status";

        static final ParseField LEADER_GLOBAL_CHECKPOINT_FIELD = new ParseField("leader_global_checkpoint");
        static final ParseField LEADER_MAX_SEQ_NO_FIELD = new ParseField("leader_max_seq_no");
        static final ParseField FOLLOWER_GLOBAL_CHECKPOINT_FIELD = new ParseField("follower_global_checkpoint");
        static final ParseField FOLLOWER_MAX_SEQ_NO_FIELD = new ParseField("follower_max_seq_no");
        static final ParseField LAST_REQUESTED_SEQ_NO_FIELD = new ParseField("last_requested_seq_no");
        static final ParseField NUMBER_OF_CONCURRENT_READS_FIELD = new ParseField("number_of_concurrent_reads");
        static final ParseField NUMBER_OF_CONCURRENT_WRITES_FIELD = new ParseField("number_of_concurrent_writes");
        static final ParseField INDEX_METADATA_VERSION_FIELD = new ParseField("index_metadata_version");
        static final ParseField TOTAL_FETCH_TIME_NANOS_FIELD = new ParseField("total_fetch_time_nanos");
        static final ParseField NUMBER_OF_FETCHES_FIELD = new ParseField("number_of_fetches");
        static final ParseField NUMBER_OF_SUCCESSFUL_FETCHES_FIELD = new ParseField("number_of_successful_fetches");
        static final ParseField OPERATIONS_RECEIVED_FIELD = new ParseField("operations_received");
        static final ParseField TOTAL_TRANSFERRED_BYTES = new ParseField("total_transferred_bytes");
        static final ParseField TOTAL_INDEX_TIME_NANOS_FIELD = new ParseField("total_index_time_nanos");
        static final ParseField NUMBER_OF_BULK_OPERATIONS_FIELD = new ParseField("number_of_bulk_operations");
        static final ParseField NUMBER_OF_SUCCESSFUL_BULK_OPERATIONS_FIELD = new ParseField("number_of_successful_bulk_operations");
        static final ParseField NUMBER_OF_OPERATIONS_INDEXED_FIELD = new ParseField("number_of_operations_indexed");
//        static final ParseField CURRENT_IDLE_TIME_FIELD = new ParseField("current_idle_time");

//        static {

//            PARSER.declareLong(ConstructingObjectParser.constructorArg(), CURRENT_IDLE_TIME_FIELD);
//        }
//
//        private final long currentIdleTime;
//
//        public final long currentIdleTime() {
//            return currentIdleTime;
//        }
//

        static final ConstructingObjectParser<Status, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new Status(
                    (long) args[0],
                    (long) args[1],
                    (long) args[2],
                    (long) args[3],
                    (long) args[4],
                    (int) args[4],
                    (int) args[5],
                    (long) args[6],
                    (long) args[7],
                    (long) args[8],
                    (long) args[10],
                    (long) args[11],
                    (long) args[12],
                    (long) args[13],
                    (long) args[14],
                    (long) args[15],
                    (long) args[16]));

        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_GLOBAL_CHECKPOINT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_MAX_SEQ_NO_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_GLOBAL_CHECKPOINT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_MAX_SEQ_NO_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_REQUESTED_SEQ_NO_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_CONCURRENT_READS_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_CONCURRENT_WRITES_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), INDEX_METADATA_VERSION_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_FETCH_TIME_NANOS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_FETCHES_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_SUCCESSFUL_FETCHES_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), OPERATIONS_RECEIVED_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_TRANSFERRED_BYTES);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_INDEX_TIME_NANOS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_BULK_OPERATIONS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_SUCCESSFUL_BULK_OPERATIONS_FIELD);
        }

        private final long leaderGlobalCheckpoint;

        public long leaderGlobalCheckpoint() {
            return leaderGlobalCheckpoint;
        }

        private final long leaderMaxSeqNo;

        public long leaderMaxSeqNo() {
            return leaderMaxSeqNo;
        }

        private final long followerGlobalCheckpoint;

        public long followerGlobalCheckpoint() {
            return followerGlobalCheckpoint;
        }

        private final long followerMaxSeqNo;

        public long followerMaxSeqNo() {
            return followerMaxSeqNo;
        }

        private final long lastRequestedSeqNo;

        public long lastRequestedSeqNo() {
            return lastRequestedSeqNo;
        }

        private final int numberOfConcurrentReads;

        public int numberOfConcurrentReads() {
            return numberOfConcurrentReads;
        }

        private final int numberOfConcurrentWrites;

        public int numberOfConcurrentWrites() {
            return numberOfConcurrentWrites;
        }

        private final long indexMetadataVersion;

        public long indexMetadataVersion() {
            return indexMetadataVersion;
        }

        private final long totalFetchTimeNanos;

        public long totalFetchTimeNanos() {
            return totalFetchTimeNanos;
        }

        private final long numberOfSuccessfulFetches;

        public long numberOfSuccessfulFetches() {
            return numberOfSuccessfulFetches;
        }

        private final long numberOfFailedFetches;

        public long numberOfFailedFetches() {
            return numberOfFailedFetches;
        }

        private final long operationsReceived;

        public long operationsReceived() {
            return operationsReceived;
        }

        private final long totalTransferredBytes;

        public long totalTransferredBytes() {
            return totalTransferredBytes;
        }

        private final long totalIndexTimeNanos;

        public long totalIndexTimeNanos() {
            return totalIndexTimeNanos;
        }

        private final long numberOfSuccessfulBulkOperations;

        public long numberOfSuccessfulBulkOperations() {
            return numberOfSuccessfulBulkOperations;
        }

        private final long numberOfFailedBulkOperations;

        public long numberOfFailedBulkOperations() {
            return numberOfFailedBulkOperations;
        }

        private final long numberOfOperationsIndexed;

        public long numberOfOperationsIndexed() {
            return numberOfOperationsIndexed;
        }

        Status(
                final long leaderGlobalCheckpoint,
                final long leaderMaxSeqNo,
                final long followerGlobalCheckpoint,
                final long followerMaxSeqNo,
                final long lastRequestedSeqNo,
                final int numberOfConcurrentReads,
                final int numberOfConcurrentWrites,
                final long indexMetadataVersion,
                final long totalFetchTimeNanos,
                final long numberOfSuccessfulFetches,
                final long numberOfFailedFetches,
                final long operationsReceived,
                final long totalTransferredBytes,
                final long totalIndexTimeNanos,
                final long numberOfSuccessfulBulkOperations,
                final long numberOfFailedBulkOperations,
                final long numberOfOperationsIndexed) {
            this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
            this.leaderMaxSeqNo = leaderMaxSeqNo;
            this.followerGlobalCheckpoint = followerGlobalCheckpoint;
            this.followerMaxSeqNo = followerMaxSeqNo;
            this.lastRequestedSeqNo = lastRequestedSeqNo;
            this.numberOfConcurrentReads = numberOfConcurrentReads;
            this.numberOfConcurrentWrites = numberOfConcurrentWrites;
            this.indexMetadataVersion = indexMetadataVersion;
            this.totalFetchTimeNanos = totalFetchTimeNanos;
            this.numberOfSuccessfulFetches = numberOfSuccessfulFetches;
            this.numberOfFailedFetches = numberOfFailedFetches;
            this.operationsReceived = operationsReceived;
            this.totalTransferredBytes = totalTransferredBytes;
            this.totalIndexTimeNanos = totalIndexTimeNanos;
            this.numberOfSuccessfulBulkOperations = numberOfSuccessfulBulkOperations;
            this.numberOfFailedBulkOperations = numberOfFailedBulkOperations;
            this.numberOfOperationsIndexed = numberOfOperationsIndexed;
        }

        public Status(final StreamInput in) throws IOException {
            this.leaderGlobalCheckpoint = in.readZLong();
            this.leaderMaxSeqNo = in.readZLong();
            this.followerGlobalCheckpoint = in.readZLong();
            this.followerMaxSeqNo = in.readZLong();
            this.lastRequestedSeqNo = in.readZLong();
            this.numberOfConcurrentReads = in.readVInt();
            this.numberOfConcurrentWrites = in.readVInt();
            this.indexMetadataVersion = in.readVLong();
            this.totalFetchTimeNanos = in.readVLong();
            this.numberOfSuccessfulFetches = in.readVLong();
            this.numberOfFailedFetches = in.readVLong();
            this.operationsReceived = in.readVLong();
            this.totalTransferredBytes = in.readVLong();
            this.totalIndexTimeNanos = in.readVLong();
            this.numberOfSuccessfulBulkOperations = in.readVLong();
            this.numberOfFailedBulkOperations = in.readVLong();
            this.numberOfOperationsIndexed = in.readVLong();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeZLong(leaderGlobalCheckpoint);
            out.writeZLong(leaderMaxSeqNo);
            out.writeZLong(followerGlobalCheckpoint);
            out.writeZLong(followerMaxSeqNo);
            out.writeZLong(lastRequestedSeqNo);
            out.writeVInt(numberOfConcurrentReads);
            out.writeVInt(numberOfConcurrentWrites);
            out.writeVLong(indexMetadataVersion);
            out.writeVLong(totalFetchTimeNanos);
            out.writeVLong(numberOfSuccessfulFetches);
            out.writeVLong(numberOfFailedFetches);
            out.writeVLong(operationsReceived);
            out.writeVLong(totalTransferredBytes);
            out.writeVLong(numberOfSuccessfulBulkOperations);
            out.writeVLong(numberOfFailedBulkOperations);
            out.writeVLong(numberOfOperationsIndexed);
//            out.writeVLong(currentIdleTime);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field(LEADER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), leaderGlobalCheckpoint);
                builder.field(LEADER_MAX_SEQ_NO_FIELD.getPreferredName(), leaderMaxSeqNo);
                builder.field(FOLLOWER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), followerGlobalCheckpoint);
                builder.field(FOLLOWER_MAX_SEQ_NO_FIELD.getPreferredName(), followerMaxSeqNo);
                builder.field(LAST_REQUESTED_SEQ_NO_FIELD.getPreferredName(), lastRequestedSeqNo);
                builder.field(NUMBER_OF_CONCURRENT_READS_FIELD.getPreferredName(), numberOfConcurrentReads);
                builder.field(NUMBER_OF_CONCURRENT_WRITES_FIELD.getPreferredName(), numberOfConcurrentWrites);
                builder.field(INDEX_METADATA_VERSION_FIELD.getPreferredName(), indexMetadataVersion);
                builder.field(TOTAL_FETCH_TIME_NANOS_FIELD.getPreferredName(), totalFetchTimeNanos);
                builder.field(NUMBER_OF_SUCCESSFUL_FETCHES_FIELD.getPreferredName(), numberOfSuccessfulFetches);
                builder.field(NUMBER_OF_FETCHES_FIELD.getPreferredName(), numberOfSuccessfulFetches + numberOfFailedFetches);
                builder.field(OPERATIONS_RECEIVED_FIELD.getPreferredName(), operationsReceived);
                builder.field(TOTAL_TRANSFERRED_BYTES.getPreferredName(), totalTransferredBytes);
                builder.field(
                        NUMBER_OF_BULK_OPERATIONS_FIELD.getPreferredName(),
                        numberOfSuccessfulBulkOperations + numberOfFailedBulkOperations);
                builder.field(NUMBER_OF_SUCCESSFUL_BULK_OPERATIONS_FIELD.getPreferredName(), numberOfSuccessfulBulkOperations);
                builder.field(NUMBER_OF_OPERATIONS_INDEXED_FIELD.getPreferredName(), numberOfOperationsIndexed);
//                builder.field(CURRENT_IDLE_TIME_FIELD.getPreferredName(), currentIdleTime);
//                builder.field(LEADER_MAX_SEQ_NO_FIELD.getPreferredName(), leaderMaxSeqNo);
//                builder.field(FOLLOWER_PRIMARY_MAX_SEQ_NO_FIELD.getPreferredName(), followerPrimaryMaxSeqNo);
            }
            builder.endObject();
            return builder;
        }

        public static Status fromXContent(final XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Status that = (Status) o;
            return leaderGlobalCheckpoint == that.leaderGlobalCheckpoint &&
                    leaderMaxSeqNo == that.leaderMaxSeqNo &&
                    followerGlobalCheckpoint == that.followerGlobalCheckpoint &&
                    followerMaxSeqNo == that.followerMaxSeqNo &&
                    lastRequestedSeqNo == that.lastRequestedSeqNo &&
                    numberOfConcurrentReads == that.numberOfConcurrentReads &&
                    numberOfConcurrentWrites == that.numberOfConcurrentWrites &&
                    indexMetadataVersion == that.indexMetadataVersion &&
                    totalFetchTimeNanos == that.totalFetchTimeNanos &&
                    numberOfSuccessfulFetches == that.numberOfSuccessfulFetches &&
                    numberOfFailedFetches == that.numberOfFailedFetches &&
                    operationsReceived == that.operationsReceived &&
                    totalTransferredBytes == that.totalTransferredBytes &&
                    numberOfSuccessfulBulkOperations == that.numberOfSuccessfulBulkOperations &&
                    numberOfFailedBulkOperations == that.numberOfFailedBulkOperations;
//                    && currentIdleTime == that.currentIdleTime
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    leaderGlobalCheckpoint,
                    leaderMaxSeqNo,
                    followerGlobalCheckpoint,
                    followerMaxSeqNo,
                    lastRequestedSeqNo,
                    numberOfConcurrentReads,
                    numberOfConcurrentWrites,
                    indexMetadataVersion,
                    totalFetchTimeNanos,
                    numberOfSuccessfulFetches,
                    numberOfFailedFetches,
                    operationsReceived,
                    totalTransferredBytes,
                    numberOfSuccessfulBulkOperations,
                    numberOfFailedBulkOperations);

        }

        public String toString() {
            return Strings.toString(this);
        }

    }

}
