/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.analytics;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AnalyticsFeatureSetUsage extends XPackFeatureSet.Usage {

    private final AnalyticsStatsAction.Response response;
    private final Map<String, Object> stats;

    public AnalyticsFeatureSetUsage(boolean available, boolean enabled, AnalyticsStatsAction.Response response) {
        super(XPackField.ANALYTICS, available, enabled);
        this.response = response;
        this.stats = response.getStats().toMap();
    }

    public AnalyticsFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        if (input.getVersion().before(Version.V_7_8_0)) {
            this.response = new AnalyticsStatsAction.Response(input);
            this.stats = this.response.getStats().toMap();
        } else {
            this.response = null;
            this.stats = input.readMap();
        }
    }

    public Version getMinimalSupportedVersion() {
        return Version.V_7_4_0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, response);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (response != null) {
            response.toXContent(builder, params);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_7_8_0)) {
            assert response != null;
            response.writeTo(out);
        } else {
            out.writeMap(stats);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AnalyticsFeatureSetUsage other = (AnalyticsFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(response, other.response);
    }
}
