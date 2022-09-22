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

package org.apache.flink.iteration.proxy;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.broadcast.OutputReflectionContext;
import org.apache.flink.iteration.typeinfo.ReusedIterationRecordSerializer;
import org.apache.flink.iteration.utils.ReflectionUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

public class ProxyOutputFactory {

    public static final ThreadLocal<IntValue> EPOCH =
            ThreadLocal.withInitial(() -> new IntValue(-1));

    public static <T> Output<StreamRecord<T>> createProxyOutput(
            Output<StreamRecord<IterationRecord<T>>> rawOutput,
            int miniBatchRecords,
            Counter numberOutputs,
            StreamConfig streamConfig) {
        try {
            OutputReflectionContext reflectionContext = new OutputReflectionContext();

            if (reflectionContext.isCopyingChainingOutput(rawOutput)) {
                return recreateChainingOutput(reflectionContext, rawOutput, true);
            } else if (reflectionContext.isChainingOutput(rawOutput)) {
                return recreateChainingOutput(reflectionContext, rawOutput, false);
            } else {
                // TODO: Let's first skip optimizing such cases.
                return new ProxyOutput<>(rawOutput);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> Output<StreamRecord<T>> recreateChainingOutput(
            OutputReflectionContext reflectionContext,
            Output<StreamRecord<IterationRecord<T>>> rawOutput,
            boolean isCopying)
            throws Exception {
        Object inputObj =
                ReflectionUtils.getFieldValue(
                        rawOutput, reflectionContext.getChainingOutputClass(), "input");

        if (!(inputObj instanceof IterationWrapperOperator)) {
            return new ProxyOutput<>(rawOutput);
        }

        IterationWrapperOperator wrapperOperator = (IterationWrapperOperator) inputObj;

        Input<T> wrappedOperator = wrapperOperator.getWrappedOperator();

        if (isCopying) {
            ReusedIterationRecordSerializer iterationRecordSerializer =
                    ReflectionUtils.getFieldValue(
                            rawOutput,
                            reflectionContext.getCopyingChainingOutputClass(),
                            "serializer");

            return (Output<StreamRecord<T>>)
                    ReflectionUtils.createInstance(
                            reflectionContext.getCopyingChainingOutputClass(),
                            Arrays.asList(
                                    Input.class,
                                    TypeSerializer.class,
                                    OperatorMetricGroup.class,
                                    OutputTag.class),
                            new Object[] {
                                wrappedOperator,
                                iterationRecordSerializer.getInnerSerializer(),
                                ((StreamOperator) wrapperOperator).getMetricGroup(),
                                reflectionContext.getChainingOutputTag(rawOutput)
                            });
        } else {
            return (Output<StreamRecord<T>>)
                    ReflectionUtils.createInstance(
                            reflectionContext.getChainingOutputClass(),
                            Arrays.asList(Input.class, OperatorMetricGroup.class, OutputTag.class),
                            new Object[] {
                                wrappedOperator,
                                ((StreamOperator) wrapperOperator).getMetricGroup(),
                                reflectionContext.getChainingOutputTag(rawOutput)
                            });
        }
    }
}
