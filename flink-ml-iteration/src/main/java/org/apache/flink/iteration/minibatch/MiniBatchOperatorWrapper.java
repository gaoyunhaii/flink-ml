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

package org.apache.flink.iteration.minibatch;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.minibatch.cache.MultiMiniBatchCache;
import org.apache.flink.iteration.minibatch.operator.wrapper.MultiInputMiniBatchWrapperOperator;
import org.apache.flink.iteration.minibatch.operator.wrapper.OneInputMiniBatchWrapperOperator;
import org.apache.flink.iteration.minibatch.operator.wrapper.TwoInputMiniBatchWrapperOperator;
import org.apache.flink.iteration.minibatch.proxy.MiniBatchCalculatedStreamPartitioner;
import org.apache.flink.iteration.minibatch.proxy.MiniBatchProxyKeySelector;
import org.apache.flink.iteration.minibatch.proxy.MiniBatchProxyStreamPartitioner;
import org.apache.flink.iteration.operator.OperatorWrapper;
import org.apache.flink.iteration.operator.WrapperOperatorFactory;
import org.apache.flink.iteration.typeinfo.IterationRecordTypeInfo;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

/** Mini-batch operator wrapper. */
public class MiniBatchOperatorWrapper<T> implements OperatorWrapper<T, MiniBatchRecord<T>> {

    private final OperatorWrapper<T, IterationRecord<T>> iterationWrapper;

    private final int miniBatchRecords;

    public MiniBatchOperatorWrapper(
            OperatorWrapper<T, IterationRecord<T>> iterationWrapper, int miniBatchRecords) {
        this.iterationWrapper = iterationWrapper;
        this.miniBatchRecords = miniBatchRecords;
    }

    public OperatorWrapper<T, IterationRecord<T>> getIterationWrapper() {
        return iterationWrapper;
    }

    public int getMiniBatchRecords() {
        return miniBatchRecords;
    }

    @Override
    public StreamOperator<MiniBatchRecord<T>> wrap(
            StreamOperatorParameters<MiniBatchRecord<T>> operatorParameters,
            StreamOperatorFactory<T> operatorFactory) {
        WrapperOperatorFactory<T> iterationOperatorFactory =
                new WrapperOperatorFactory<>(operatorFactory, iterationWrapper);
        Class<? extends StreamOperator> operatorClass =
                operatorFactory.getStreamOperatorClass(getClass().getClassLoader());
        if (OneInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return new OneInputMiniBatchWrapperOperator<>(
                    operatorParameters, iterationOperatorFactory, miniBatchRecords);
        } else if (TwoInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return new TwoInputMiniBatchWrapperOperator<>(
                    operatorParameters, iterationOperatorFactory, miniBatchRecords);
        } else if (MultipleInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return new MultiInputMiniBatchWrapperOperator<>(
                    operatorParameters, iterationOperatorFactory, miniBatchRecords);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported operator class for all-round wrapper: " + operatorClass);
        }
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(
            ClassLoader classLoader, StreamOperatorFactory<T> operatorFactory) {
        Class<? extends StreamOperator> operatorClass =
                operatorFactory.getStreamOperatorClass(getClass().getClassLoader());
        if (OneInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return OneInputMiniBatchWrapperOperator.class;
        } else if (TwoInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return TwoInputMiniBatchWrapperOperator.class;
        } else if (MultipleInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return MultiInputMiniBatchWrapperOperator.class;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported operator class for all-round wrapper: " + operatorClass);
        }
    }

    @Override
    public <KEY> KeySelector<MiniBatchRecord<T>, KEY> wrapKeySelector(
            KeySelector<T, KEY> keySelector) {
        // For key selector, let's use the first one
        return new MiniBatchProxyKeySelector<>(keySelector);
    }

    @Override
    public StreamPartitioner<MiniBatchRecord<T>> wrapStreamPartitioner(
            StreamPartitioner<T> streamPartitioner) {

        if (streamPartitioner == null
                || streamPartitioner instanceof ForwardPartitioner
                || streamPartitioner instanceof RescalePartitioner
                || streamPartitioner instanceof RebalancePartitioner) {
            return new MiniBatchProxyStreamPartitioner<>(streamPartitioner);
        } else {
            return new MiniBatchCalculatedStreamPartitioner<>(streamPartitioner);
        }
    }

    @Override
    public OutputTag<MiniBatchRecord<T>> wrapOutputTag(OutputTag<T> outputTag) {
        return new OutputTag<>(
                outputTag.getId(),
                new MiniBatchRecordTypeInfo<>(
                        new IterationRecordTypeInfo<>(outputTag.getTypeInfo(), true)));
    }

    @Override
    public TypeInformation<MiniBatchRecord<T>> getWrappedTypeInfo(TypeInformation<T> typeInfo) {
        return new MiniBatchRecordTypeInfo<>(new IterationRecordTypeInfo<>(typeInfo, true));
    }
}
