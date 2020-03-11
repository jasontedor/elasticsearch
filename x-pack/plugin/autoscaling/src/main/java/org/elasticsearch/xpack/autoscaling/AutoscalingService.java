/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import java.util.Collection;
import java.util.stream.Collectors;

public class AutoscalingService {

    public AutoscalingService() {}

    public AutoscalingDecision scale(final Collection<AutoscalingDecider> autoscalingDeciders) {
        final Collection<AutoscalingDecision> decisions = autoscalingDeciders.stream()
            .map(AutoscalingDecider::scale)
            .collect(Collectors.toUnmodifiableList());
        return new AutoscalingDecision.MultipleAutoscalingDecision(decisions);
    }

}
