/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.iteration;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The config for an iteration. */
public class IterationConfig {

    /** The default operator lifecycle. */
    private final OperatorLifeCycle operatorLifeCycle;

    private final int miniBatchRecords;

    public IterationConfig(OperatorLifeCycle operatorLifeCycle, int miniBatchRecords) {
        this.operatorLifeCycle = operatorLifeCycle;
        this.miniBatchRecords = miniBatchRecords;
    }

    public static IterationConfigBuilder newBuilder() {

        return new IterationConfigBuilder();
    }

    public OperatorLifeCycle getOperatorLifeCycle() {
        return operatorLifeCycle;
    }

    public int getMiniBatchRecords() {
        return miniBatchRecords;
    }

    /** The builder of the {@link IterationConfig}. */
    public static class IterationConfigBuilder {

        private OperatorLifeCycle operatorLifeCycle = OperatorLifeCycle.ALL_ROUND;

        private int miniBatchRecords = 1;

        private IterationConfigBuilder() {}

        public IterationConfigBuilder setOperatorLifeCycle(OperatorLifeCycle operatorLifeCycle) {
            this.operatorLifeCycle = operatorLifeCycle;
            return this;
        }

        public IterationConfigBuilder setMiniBatchRecords(int miniBatchRecords) {
            checkArgument(miniBatchRecords >= 1, "mini-batch size must be positive");
            this.miniBatchRecords = miniBatchRecords;
            return this;
        }

        public IterationConfig build() {
            return new IterationConfig(operatorLifeCycle, miniBatchRecords);
        }
    }

    /** LifeCycles of the operator inside the iteration. */
    public enum OperatorLifeCycle {

        /** The operator lives for all the rounds until termination. */
        ALL_ROUND,

        /** The operator will be recreated in each round. */
        PER_ROUND
    }
}
