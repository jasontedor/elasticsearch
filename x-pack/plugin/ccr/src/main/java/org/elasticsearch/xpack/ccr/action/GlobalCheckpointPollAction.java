package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class GlobalCheckpointPollAction extends Action<GlobalCheckpointPollAction.Response> {

    public static final GlobalCheckpointPollAction INSTANCE = new GlobalCheckpointPollAction();
    public static final String NAME = "indices:data/read/xpack/ccr/global_checkpoint_poll";

    protected GlobalCheckpointPollAction() {
        super(NAME);
    }

    public static class Request extends SingleShardRequest<Request> {

        private long globalCheckpoint = UNASSIGNED_SEQ_NO;

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        public void setGlobalCheckpoint(long globalCheckpoint) {
            this.globalCheckpoint = globalCheckpoint;
        }

        private ShardId shardId;

        public ShardId getShardId() {
            return shardId;
        }

        public Request() {

        }

        public Request(final ShardId shardId) {
            super(shardId.getIndexName());
            this.shardId = shardId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (globalCheckpoint < NO_OPS_PERFORMED) {
                e = addValidationError("global checkpoint [" + globalCheckpoint + "] must be at least -1", e);
            }
            if (shardId == null) {
                e = addValidationError("shard ID must be non-null", e);
            }
            return e;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            globalCheckpoint = in.readZLong();
            shardId = ShardId.readShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(globalCheckpoint);
            shardId.writeTo(out);
        }

    }

    public static class Response extends ActionResponse {

        private long globalCheckpoint;

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        public Response() {

        }

        public Response(final long globalCheckpoint) {
            this.globalCheckpoint = globalCheckpoint;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            globalCheckpoint = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(globalCheckpoint);
        }

    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class TransportGlobalCheckpointPollAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportGlobalCheckpointPollAction(
                final Settings settings,
                final ThreadPool threadPool,
                final ClusterService clusterService,
                final TransportService transportService,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver,
                final IndicesService indicesService) {
            super(
                    settings,
                    NAME,
                    threadPool,
                    clusterService,
                    transportService,
                    actionFilters,
                    indexNameExpressionResolver,
                    Request::new,
                    ThreadPool.Names.MANAGEMENT);
            this.indicesService = indicesService;
        }

        @Override
        protected void asyncShardOperation(
                final Request request, final ShardId shardId, final ActionListener<Response> listener) throws IOException {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            indexShard.addGlobalCheckpointListener(
                    request.getGlobalCheckpoint(),
                    (g, e) -> {
                        if (g != UNASSIGNED_SEQ_NO) {
                            assert g >= NO_OPS_PERFORMED && e == null;
                            listener.onResponse(new Response(g));
                        } else {
                            assert e != null;
                            listener.onFailure(e);
                        }
            });
        }

        @Override
        protected Response shardOperation(final Request request, final ShardId shardId) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected boolean resolveIndex(final Request request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(final ClusterState state, final InternalRequest request) {
            return state
                    .routingTable()
                    .shardRoutingTable(request.concreteIndex(), request.request().getShardId().id())
                    .activeInitializingShardsRandomIt();
        }

    }

}
