/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public abstract class AutoscalingDecision implements ToXContent, Writeable {

    public abstract String name();

    public abstract Type type();

    public abstract String reason();

    public abstract Collection<AutoscalingDecision> decisions();

    public static AutoscalingDecision readFrom(final StreamInput in) throws IOException {
        if (in.readBoolean()) {
            final Collection<AutoscalingDecision> decisions = in.readList(AutoscalingDecision::readFrom);
            return new MultipleAutoscalingDecision(decisions);
        } else {
            final String name = in.readString();
            final Type type = Type.readFrom(in);
            final String reason = in.readString();
            return new SingleAutoscalingDecision(name, type, reason);
        }
    }

    public static class SingleAutoscalingDecision extends AutoscalingDecision {

        private final String name;

        @Override
        public String name() {
            return name;
        }

        private final Type type;

        @Override
        public Type type() {
            return type;
        }

        private final String reason;

        @Override
        public String reason() {
            return reason;
        }

        @Override
        public Collection<AutoscalingDecision> decisions() {
            return List.of(this);
        }

        public SingleAutoscalingDecision(final String name, final Type type, final String reason) {
            this.name = name;
            this.type = type;
            this.reason = reason;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeBoolean(false);
            out.writeString(name);
            type.writeTo(out);
            out.writeString(reason);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", name);
                builder.field("type", type);
                builder.field("reason", reason);
            }
            builder.endObject();
            return builder;
        }

    }

    public static class MultipleAutoscalingDecision extends AutoscalingDecision {

        private final Collection<AutoscalingDecision> decisions;

        @Override
        public String name() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type type() {
            if (decisions.stream().anyMatch(p -> p.type() == Type.YES)) {
                return Type.YES;
            } else {
                return Type.NO;
            }
        }

        @Override
        public String reason() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<AutoscalingDecision> decisions() {
            return decisions;
        }

        public MultipleAutoscalingDecision(final Collection<AutoscalingDecision> decisions) {
            this.decisions = decisions;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeBoolean(true);
            out.writeCollection(decisions);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            for (final AutoscalingDecision decision : decisions) {
                decision.toXContent(builder, params);
            }
            return builder;
        }
    }

    public enum Type implements Writeable {
        NO((byte) 0),
        YES((byte) 1);

        private final byte id;

        Type(final byte id) {
            this.id = id;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static Type readFrom(final StreamInput in) throws IOException {
            final byte id = in.readByte();
            switch (id) {
                case 0:
                    return NO;
                case 1:
                    return YES;
                default:
                    throw new IllegalArgumentException("unexpected value [" + id + "] for autoscaling decision type");
            }
        }

    }

}
