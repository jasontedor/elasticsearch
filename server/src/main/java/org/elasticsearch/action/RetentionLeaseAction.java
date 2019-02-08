package org.elasticsearch.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class RetentionLeaseAction extends Action<RetentionLeaseAction.Response> {

    public static final RetentionLeaseAction INSTANCE = new RetentionLeaseAction();
    public static final String NAME = "indices:data/write/retention_lease";

    private RetentionLeaseAction() {
        super(NAME);
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(
                final ThreadPool threadPool,
                final ClusterService clusterService,
                final TransportService transportService,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver,
                final IndicesService indicesService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, Request::new, ThreadPool.Names.MANAGEMENT);
            this.indicesService = Objects.requireNonNull(indicesService);
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state
                    .routingTable()
                    .shardRoutingTable(request.concreteIndex(), request.request().getShardId().id())
                    .primaryShardIt();
        }

        @Override
        protected Response shardOperation(final Request request, final ShardId shardId) {
            final IndexService indexService = indicesService.indexServiceSafe(request.getShardId().getIndex());
            final IndexShard indexShard = indexService.getShard(request.getShardId().id());

            final CompletableFuture<Releasable> permit = new CompletableFuture<>();
            final ActionListener<Releasable> onAcquired = new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    if (permit.complete(releasable) == false) {
                        releasable.close();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    permit.completeExceptionally(e);
                }
            };
            indexShard.acquirePrimaryOperationPermit(onAcquired, ThreadPool.Names.SAME, request);
            try (Releasable ignore = FutureUtils.get(permit)) {
                indexShard.addRetentionLease(request.getId(), request.getRetainingSequenceNumber(), request.getSource(), ActionListener.wrap(() -> {}));
            } finally {
                // just in case we got an exception (likely interrupted) while waiting for the get
                permit.whenComplete((r, e) -> {
                    if (r != null) {
                        r.close();
                    }
                    if (e != null) {
                        logger.trace("suppressing exception on completion (it was already bubbled up or the operation was aborted)", e);
                    }
                });
            }

            return new Response();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected boolean resolveIndex(final Request request) {
            return false;
        }

    }

    public static class Request extends SingleShardRequest<Request> {

        public static long RETAIN_ALL = -1;

        private ShardId shardId;

        public ShardId getShardId() {
            return shardId;
        }

        private String id;

        public String getId() {
            return id;
        }

        private long retainingSequenceNumber;

        public long getRetainingSequenceNumber() {
            return retainingSequenceNumber;
        }

        private String source;

        public String getSource() {
            return source;
        }

        public Request() {
        }

        public Request(final ShardId shardId, final String id, final long retainingSequenceNumber, final String source) {
            super(Objects.requireNonNull(shardId).getIndexName());
            this.shardId = shardId;
            this.id = Objects.requireNonNull(id);
            if (retainingSequenceNumber < -1) {
                throw new IllegalArgumentException(
                        "retention lease retaining sequence number [" + retainingSequenceNumber + "] out of range");
            }
            this.retainingSequenceNumber = retainingSequenceNumber;
            this.source = Objects.requireNonNull(source);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            id = in.readString();
            retainingSequenceNumber = in.readZLong();
            source = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(id);
            out.writeZLong(retainingSequenceNumber);
            out.writeString(source);
        }

    }

    public static class Response extends ActionResponse {

    }

    @Override
    public Response newResponse() {
        return new Response();
    }


}
