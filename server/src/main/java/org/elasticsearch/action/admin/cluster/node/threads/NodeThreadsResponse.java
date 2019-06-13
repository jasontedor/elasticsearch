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

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.lang.management.ThreadInfo;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class NodeThreadsResponse extends BaseNodeResponse {

    private Map<Long, ThreadInfo> threads;

    public Map<Long, ThreadInfo> getThreads() {
        return threads;
    }

    private Set<LinkedHashSet<Long>> deadlocks;

    public Set<LinkedHashSet<Long>> getDeadlocks() {
        return deadlocks;
    }

    public NodeThreadsResponse() {

    }

    public NodeThreadsResponse(final DiscoveryNode node, final Map<Long, ThreadInfo> threads, final Set<LinkedHashSet<Long>> deadlocks) {
        super(node);
        this.threads = Objects.requireNonNull(threads);
        this.deadlocks = Objects.requireNonNull(deadlocks);
    }

    public static NodeThreadsResponse read(final StreamInput in) throws IOException {
        final NodeThreadsResponse node = new NodeThreadsResponse();
        node.readFrom(in);
        return node;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
    }

}
