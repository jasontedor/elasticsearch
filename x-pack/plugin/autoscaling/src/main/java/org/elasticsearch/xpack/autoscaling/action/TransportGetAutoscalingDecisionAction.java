/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TransportGetAutoscalingDecisionAction extends TransportMasterNodeAction<
    GetAutoscalingDecisionAction.Request,
    GetAutoscalingDecisionAction.Response> {

    private final AutoscalingService autoscalingService;

    @Inject
    public TransportGetAutoscalingDecisionAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AutoscalingService autoscalingService
    ) {
        super(
            GetAutoscalingDecisionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAutoscalingDecisionAction.Request::new,
            indexNameExpressionResolver
        );
        this.autoscalingService = Objects.requireNonNull(autoscalingService);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetAutoscalingDecisionAction.Response read(final StreamInput in) throws IOException {
        return new GetAutoscalingDecisionAction.Response(in);
    }

    @Override
    protected void masterOperation(
        final Task task,
        final GetAutoscalingDecisionAction.Request request,
        final ClusterState state,
        final ActionListener<GetAutoscalingDecisionAction.Response> listener
    ) {
        final AutoscalingMetadata metadata;
        if (state.metaData().custom(AutoscalingMetadata.NAME) != null) {
            metadata = state.metaData().custom(AutoscalingMetadata.NAME);
        } else {
            metadata = AutoscalingMetadata.EMPTY;
        }
        final Map<String, AutoscalingDecision> decisions = metadata.policies()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> autoscalingService.scale(e.getValue().policy().deciders())));
        listener.onResponse(new GetAutoscalingDecisionAction.Response(new TreeMap<>(decisions)));
    }

    @Override
    protected ClusterBlockException checkBlock(final GetAutoscalingDecisionAction.Request request, final ClusterState state) {
        return null;
    }

}
