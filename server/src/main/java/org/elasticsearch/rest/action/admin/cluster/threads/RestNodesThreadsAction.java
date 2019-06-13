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

package org.elasticsearch.rest.action.admin.cluster.threads;

import org.elasticsearch.action.admin.cluster.node.threads.NodeThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.threads.NodesThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.threads.NodesThreadsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class RestNodesThreadsAction extends BaseRestHandler {

    public RestNodesThreadsAction(final Settings settings, final RestController controller) {
        super(Objects.requireNonNull(settings));
        Objects.requireNonNull(controller);
        controller.registerHandler(RestRequest.Method.GET, "/_nodes/threads", this);
        controller.registerHandler(RestRequest.Method.GET, "_nodes/{node_id}/threads", this);
    }

    @Override
    public String getName() {
        return "nodes_threads_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final NodesThreadsRequest nodesThreadsRequest = new NodesThreadsRequest(nodesIds);
        return channel -> client.admin().cluster().nodesThreads(
                nodesThreadsRequest,
                new RestResponseListener<>(channel) {

                    @Override
                    public RestResponse buildResponse(final NodesThreadsResponse nodesThreadsResponse) throws IOException {
                        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                            builder.startObject();
                            {
                                builder.startArray("nodes");
                                {
                                    final List<NodeThreadsResponse> nodes = nodesThreadsResponse.getNodes();
                                    for (final NodeThreadsResponse node : nodes) {
                                        builder.startObject();
                                        {
                                            builder.startArray("threads");
                                            {
                                                final Map<Long, ThreadInfo> threads = node.getThreads();
                                                for (final Map.Entry<Long, ThreadInfo> thread : threads.entrySet()) {
                                                    builder.startObject();
                                                    {
                                                        final Map<Integer, List<MonitorInfo>> monitors =
                                                                Arrays.stream(thread.getValue().getLockedMonitors())
                                                                        .collect(Collectors.groupingBy(MonitorInfo::getLockedStackDepth, Collectors.toList()));
                                                        final LockInfo waiting = thread.getValue().getLockInfo();;
                                                        builder.field("id", thread.getKey());
                                                        builder.field("name", thread.getValue().getThreadName());
                                                        builder.field("state", thread.getValue().getThreadState());
                                                        builder.field("is_in_native", thread.getValue().isInNative());
                                                        builder.field("is_suspended", thread.getValue().isSuspended());
                                                        builder.field("is_daemon", thread.getValue().isDaemon());
                                                        builder.startArray("stacktrace");
                                                        {
                                                            final StackTraceElement[] elements = thread.getValue().getStackTrace();
                                                            for (int i = 0; i < elements.length; i++) {
                                                                final StackTraceElement element = elements[i];
                                                                final String lineNumber;
                                                                if (element.isNativeMethod()) {
                                                                    lineNumber = "native";
                                                                } else {
                                                                    lineNumber = String.format(
                                                                            Locale.ROOT,
                                                                            "%s:%s",
                                                                            element.getFileName(),
                                                                            element.getLineNumber());
                                                                }
                                                                final String location;
                                                                if (element.getModuleName() == null) {
                                                                    location = lineNumber;
                                                                } else {
                                                                    location = String.format(
                                                                            Locale.ROOT,
                                                                            "%s@%s/%s",
                                                                            element.getModuleName(),
                                                                            element.getModuleVersion(),
                                                                            lineNumber);
                                                                }
                                                                final String line = String.format(
                                                                        Locale.ROOT,
                                                                        "at %s.%s(%s)",
                                                                        element.getClassName(),
                                                                        element.getMethodName(),
                                                                        location);
                                                                builder.value(line);
                                                                if (i == 0 && waiting != null) {
                                                                    builder.value("- parking to wait for <" + waiting.toString() + ">");
                                                                }
                                                                if (monitors.get(i) != null) {
                                                                    final List<MonitorInfo> monitorInfos = monitors.get(i);
                                                                    for (final MonitorInfo monitorInfo : monitorInfos) {
                                                                        builder.value("locked <" + monitorInfo.toString() + ">");
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        builder.endArray();
                                                    }
                                                    builder.endObject();
                                                }
                                            }
                                            builder.endArray();
                                            builder.startArray("deadlocked_threads");
                                            {

                                            }
                                            builder.endArray();
                                        }
                                        builder.endObject();
                                    }
                                }
                                builder.endArray();
                            }
                            builder.endObject();
                            return new BytesRestResponse(RestStatus.OK, builder);
                        }
                    }

                });
    }

}
