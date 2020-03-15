/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AutoscalingMetadata implements MetaData.Custom {

    public static final String NAME = "autoscaling";

    public static final ParseField POLICIES_FIELD = new ParseField("policies");
    public static final AutoscalingMetadata EMPTY = new AutoscalingMetadata(Collections.emptySortedMap());

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<AutoscalingMetadata, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        c -> new AutoscalingMetadata(
            new TreeMap<>(
                ((List<AutoscalingPolicyMetadata>) c[0]).stream().collect(Collectors.toMap(p -> p.policy().name(), Function.identity()))
            )
        )
    );

    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> AutoscalingPolicyMetadata.parse(p, n),
            POLICIES_FIELD
        );
    }

    private final SortedMap<String, AutoscalingPolicyMetadata> policies;

    public SortedMap<String, AutoscalingPolicyMetadata> policies() {
        return policies;
    }

    public AutoscalingMetadata(final SortedMap<String, AutoscalingPolicyMetadata> policies) {
        this.policies = policies;
    }

    public AutoscalingMetadata(final StreamInput in) throws IOException {
        final int size = in.readVInt();
        final SortedMap<String, AutoscalingPolicyMetadata> policies = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            policies.put(in.readString(), new AutoscalingPolicyMetadata(in));
        }
        this.policies = policies;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(policies.size());
        for (final Map.Entry<String, AutoscalingPolicyMetadata> policy : policies.entrySet()) {
            out.writeString(policy.getKey());
            policy.getValue().writeTo(out);
        }
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.ALL_CONTEXTS;
    }

    @Override
    public Diff<MetaData.Custom> diff(final MetaData.Custom previousState) {
        return new AutoscalingMetadataDiff((AutoscalingMetadata) previousState, this);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        // TODO: update on backport
        return Version.V_8_0_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(POLICIES_FIELD.getPreferredName(), policies);
        return builder;
    }

    public static class AutoscalingMetadataDiff implements NamedDiff<MetaData.Custom> {

        final Diff<Map<String, AutoscalingPolicyMetadata>> policies;

        public AutoscalingMetadataDiff(final AutoscalingMetadata before, final AutoscalingMetadata after) {
            this.policies = DiffableUtils.diff(before.policies, after.policies, DiffableUtils.getStringKeySerializer());
        }

        public AutoscalingMetadataDiff(final StreamInput in) throws IOException {
            this.policies = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                AutoscalingPolicyMetadata::new,
                AutoscalingMetadataDiff::readFrom
            );
        }

        @Override
        public MetaData.Custom apply(final MetaData.Custom part) {
            final TreeMap<String, AutoscalingPolicyMetadata> newPolicies = new TreeMap<>(
                policies.apply(((AutoscalingMetadata) part).policies)
            );
            return new AutoscalingMetadata(newPolicies);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            policies.writeTo(out);
        }

        static Diff<AutoscalingPolicyMetadata> readFrom(final StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(AutoscalingPolicyMetadata::new, in);
        }

    }

}
