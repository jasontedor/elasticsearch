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

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransportNodesThreadsAction extends TransportNodesAction<NodesThreadsRequest,
        NodesThreadsResponse,
        TransportNodesThreadsAction.NodeThreadsRequest,
        NodeThreadsResponse> {

    private final ThreadMXBean mx = ManagementFactory.getThreadMXBean();

    @Inject
    public TransportNodesThreadsAction(
            final ThreadPool threadPool,
            final ClusterService clusterService,
            final TransportService transportService,
            final ActionFilters actionFilters) {
        super(
                NodesThreadsAction.ACTION_NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                NodesThreadsRequest::new,
                NodeThreadsRequest::new,
                ThreadPool.Names.MANAGEMENT,
                NodeThreadsResponse.class);
    }

    @Override
    protected NodesThreadsResponse newResponse(
            final NodesThreadsRequest request,
            final List<NodeThreadsResponse> nodes,
            final List<FailedNodeException> failures) {
        return new NodesThreadsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected NodeThreadsRequest newNodeRequest(final String nodeId, final NodesThreadsRequest request) {
        return new NodeThreadsRequest();
    }

    @Override
    protected NodeThreadsResponse newNodeResponse() {
        return new NodeThreadsResponse();
    }

    @Override
    protected NodeThreadsResponse nodeOperation(final NodeThreadsRequest request) {
        final Map<Long, ThreadInfo> threads = threads(mx);
        final Set<LinkedHashSet<Long>> deadlocks = findDeadlocks(threads);
        return new NodeThreadsResponse(clusterService.localNode(), threads, deadlocks);
    }

    static Map<Long, ThreadInfo> threads(final ThreadMXBean mx) {
        return Arrays.stream(mx.dumpAllThreads(true, true))
                .collect(Collectors.toUnmodifiableMap(ThreadInfo::getThreadId, Function.identity()));
    }

    static Set<LinkedHashSet<Long>> findDeadlocks(final Map<Long, ThreadInfo> threads) {
        final Set<LinkedHashSet<Long>> cycles = new HashSet<>();
        final Set<Long> visited = new HashSet<>();
        for (final Map.Entry<Long, ThreadInfo> entry : threads.entrySet()) {
            if (visited.contains(entry.getKey())) {
                continue;
            }
            final LinkedHashSet<Long> cycle = new LinkedHashSet<>();
            for (
                    ThreadInfo thread = entry.getValue();
                    thread != null && cycle.contains(thread.getThreadId()) == false;
                    thread = threads.get(thread.getLockOwnerId())) {
                visited.add(thread.getThreadId());
                cycle.add(thread.getThreadId());
            }

            if (cycle.size() > 1) {
                cycles.add(cycle);
            }
        }

        return cycles;
    }

    public static class NodeThreadsRequest extends BaseNodeRequest {

        NodesThreadsRequest request;

        public NodeThreadsRequest() {
        }

        NodeThreadsRequest(final String nodeId, final NodesThreadsRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            request = new NodesThreadsRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }

    }

}
