package org.elasticsearch.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

public class RetentionLeaseAction extends Action<RetentionLeaseAction.Response> {

    public static final RetentionLeaseAction INSTANCE = new RetentionLeaseAction();
    public static final String NAME = "indices:data/write/retention_lease";

    protected RetentionLeaseAction() {
        super(NAME);
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        protected TransportAction(
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
        protected Response shardOperation(final Request request, final ShardId shardId) throws IOException {
            final IndexService indexService = indicesService.indexServiceSafe(request.getShardId().getIndex());
            final IndexShard indexShard = indexService.getShard(request.getShardId().id());
            indexShard.acquireRetentionLockForPeerRecovery();
            indexShard.addRetentionLease(request.getId(), request.getRetainingSequenceNumber(), request.getSource(), ActionListener.wrap(() -> {}));
            return new Response();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return null;
        }

    }

    public static class Request extends SingleShardRequest<Request> {

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
            this.shardId = Objects.requireNonNull(shardId);
            this.id = Objects.requireNonNull(id);
            if (retainingSequenceNumber < 0) {
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
            retainingSequenceNumber = in.readVLong();
            source = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(id);
            out.writeVLong(retainingSequenceNumber);
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
