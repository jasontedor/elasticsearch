/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.AutoscalingPolicy;

import java.io.IOException;

public class PutAutoscalingPolicyAction extends ActionType<PutAutoscalingPolicyAction.Response> {

    public static final PutAutoscalingPolicyAction INSTANCE = new PutAutoscalingPolicyAction();
    public static final String NAME = "cluster:admin/autoscaling/put_autoscaling_policy";

    private PutAutoscalingPolicyAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<PutAutoscalingPolicyAction.Request> implements ToXContentObject {

        static final ParseField POLICY_FIELD = new ParseField("policy");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "put_autoscaling_policy_request",
            a -> new Request((AutoscalingPolicy) a[0])
        );

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), AutoscalingPolicy::parse, POLICY_FIELD);
        }

        public static Request parse(final String name, final XContentParser parser) {
            return PARSER.apply(parser, name);
        }

        private final AutoscalingPolicy policy;

        public AutoscalingPolicy policy() {
            return policy;
        }

        public Request(final AutoscalingPolicy policy) {
            this.policy = policy;
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            policy = new AutoscalingPolicy(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            policy.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field(POLICY_FIELD.getPreferredName(), policy);
            }
            builder.endObject();
            return builder;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public Response() {

        }

        public Response(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(final StreamOutput out) {

        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {

            }
            builder.endObject();
            return builder;
        }

    }

}
