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

package org.apache.flink.iteration.minibatch.operator;

import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.iteration.minibatch.operator.wrapper.OneInputMiniBatchWrapperOperator;
import org.apache.flink.iteration.operator.HeadOperatorFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

public class MiniBatchHeadOperatorWrapperFactory
        extends AbstractStreamOperatorFactory<MiniBatchRecord<?>>
        implements OneInputStreamOperatorFactory<MiniBatchRecord<?>, MiniBatchRecord<?>>,
                CoordinatedOperatorFactory<MiniBatchRecord<?>>,
                YieldingOperatorFactory<MiniBatchRecord<?>> {

    private final HeadOperatorFactory headOperatorFactory;

    private final int miniBatchRecords;

    public MiniBatchHeadOperatorWrapperFactory(
            HeadOperatorFactory headOperatorFactory, int miniBatchRecords) {
        this.headOperatorFactory = headOperatorFactory;
        this.miniBatchRecords = miniBatchRecords;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String s, OperatorID operatorID) {
        return headOperatorFactory.getCoordinatorProvider(s, operatorID);
    }

    @Override
    public <T extends StreamOperator<MiniBatchRecord<?>>> T createStreamOperator(
            StreamOperatorParameters<MiniBatchRecord<?>> streamOperatorParameters) {
        return (T)
                new OneInputMiniBatchWrapperOperator(
                        streamOperatorParameters, headOperatorFactory, miniBatchRecords);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return OneInputMiniBatchWrapperOperator.class;
    }
}
