/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.registry.TargetType;

import java.util.Collections;
import java.util.List;

/**
 * Starts the {@link com.hazelcast.simulator.test.TestPhase#RUN} phase of a Simulator Test.
 */
public class StartTestOperation implements SimulatorOperation {

    private final TargetType targetType;
    private final List<String> targetWorkers;

    public StartTestOperation() {
        this(TargetType.ALL);
    }

    public StartTestOperation(TargetType targetType) {
        this(targetType, Collections.<String>emptyList());
    }

    public StartTestOperation(TargetType targetType, List<String> targetWorkers) {
        this.targetType = targetType;
        this.targetWorkers = targetWorkers;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    public List<String> getTargetWorkers() {
        return targetWorkers;
    }

    public boolean hasTargetWorkers() {
        return !targetWorkers.isEmpty();
    }
}
