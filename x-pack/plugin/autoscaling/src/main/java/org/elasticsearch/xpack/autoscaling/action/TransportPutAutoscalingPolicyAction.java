/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingPolicyMetadata;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TransportPutAutoscalingPolicyAction extends TransportMasterNodeAction<
    PutAutoscalingPolicyAction.Request,
    PutAutoscalingPolicyAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutAutoscalingPolicyAction.class);

    @Inject
    public TransportPutAutoscalingPolicyAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutAutoscalingPolicyAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutAutoscalingPolicyAction.Request::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutAutoscalingPolicyAction.Response read(final StreamInput in) throws IOException {
        return new PutAutoscalingPolicyAction.Response(in);
    }

    @Override
    protected void masterOperation(
        final Task task,
        final PutAutoscalingPolicyAction.Request request,
        final ClusterState state,
        final ActionListener<PutAutoscalingPolicyAction.Response> listener
    ) {
        clusterService.submitStateUpdateTask("put-autoscaling-policy", new AckedClusterStateUpdateTask<>(request, listener) {

            @Override
            protected PutAutoscalingPolicyAction.Response newResponse(final boolean acknowledged) {
                return new PutAutoscalingPolicyAction.Response();
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                final ClusterState.Builder builder = ClusterState.builder(currentState);
                final AutoscalingMetadata currentMetadata;
                if (currentState.metaData().custom(AutoscalingMetadata.NAME) != null) {
                    currentMetadata = currentState.metaData().custom(AutoscalingMetadata.NAME);
                } else {
                    currentMetadata = AutoscalingMetadata.EMPTY;
                }
                final SortedMap<String, AutoscalingPolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.policies());
                final AutoscalingPolicyMetadata policy = new AutoscalingPolicyMetadata(request.policy());
                final AutoscalingPolicyMetadata oldPolicy = newPolicies.put(request.policy().name(), policy);
                if (oldPolicy == null) {
                    logger.info("adding autoscaling policy [{}]", request.policy().name());
                } else {
                    logger.info("updating autoscaling policy [{}]", request.policy().name());
                }
                final AutoscalingMetadata newMetadata = new AutoscalingMetadata(newPolicies);
                builder.metaData(MetaData.builder(currentState.getMetaData()).putCustom(AutoscalingMetadata.NAME, newMetadata).build());
                return builder.build();
            }

        });
    }

    @Override
    protected ClusterBlockException checkBlock(final PutAutoscalingPolicyAction.Request request, final ClusterState state) {
        return null;
    }

}
