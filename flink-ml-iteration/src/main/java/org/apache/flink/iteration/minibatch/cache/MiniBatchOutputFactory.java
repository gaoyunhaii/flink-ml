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

package org.apache.flink.iteration.minibatch.cache;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.broadcast.OutputReflectionContext;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.iteration.minibatch.ReusedMiniBatchRecordSerializer;
import org.apache.flink.iteration.minibatch.operator.WrapperOperator;
import org.apache.flink.iteration.utils.ReflectionUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

public class MiniBatchOutputFactory {

    public static Output<StreamRecord<IterationRecord<?>>> createIterationRecordOutput(
            Output<StreamRecord<MiniBatchRecord<?>>> rawOutput,
            int miniBatchRecords,
            Counter numberOutputs,
            StreamConfig streamConfig) {
        try {
            OutputReflectionContext reflectionContext = new OutputReflectionContext();
            if (reflectionContext.isCopyingChainingOutput(rawOutput)) {
                return recreateChainingOutput(reflectionContext, rawOutput, true);
            } else if (reflectionContext.isChainingOutput(rawOutput)) {
                return recreateChainingOutput(reflectionContext, rawOutput, false);
            } else if (reflectionContext.isRecordWriterOutput(rawOutput)) {
                return new MiniBatchedRecordWriterOutput(
                        reflectionContext,
                        rawOutput,
                        miniBatchRecords,
                        numberOutputs,
                        streamConfig);
            } else if (reflectionContext.isBroadcastingOutput(rawOutput)) {
                Output[] outputs = reflectionContext.getBroadcastingInternalOutputs(rawOutput);
                boolean isCopying = reflectionContext.isCopyingBroadcastingOutput(rawOutput);

                Output[] wrappedOutputs = new Output[outputs.length];
                for (int i = 0; i < wrappedOutputs.length; ++i) {
                    wrappedOutputs[i] =
                            createIterationRecordOutput(
                                    outputs[i], miniBatchRecords, numberOutputs, streamConfig);
                }

                if (isCopying) {
                    return (Output<StreamRecord<IterationRecord<?>>>)
                            ReflectionUtils.createInstance(
                                    reflectionContext.getCopyingBroadcastingOutputClass(),
                                    Arrays.asList(Output[].class),
                                    new Object[] {wrappedOutputs});
                } else {
                    return (Output<StreamRecord<IterationRecord<?>>>)
                            ReflectionUtils.createInstance(
                                    reflectionContext.getBroadcastingOutputClass(),
                                    Arrays.asList(Output[].class),
                                    new Object[] {wrappedOutputs});
                }
            } else {
                throw new RuntimeException("Unknow output type " + rawOutput);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Output<StreamRecord<IterationRecord<?>>> recreateChainingOutput(
            OutputReflectionContext reflectionContext,
            Output<StreamRecord<MiniBatchRecord<?>>> rawOutput,
            boolean isCopying)
            throws Exception {
        // Get the original operator
        WrapperOperator wrapperOperator =
                ReflectionUtils.getFieldValue(
                        rawOutput, reflectionContext.getChainingOutputClass(), "input");

        Input<IterationRecord<?>> wrappedOperator = wrapperOperator.getWrappedOperator();

        if (isCopying) {
            ReusedMiniBatchRecordSerializer miniBatchRecordSerializer =
                    ReflectionUtils.getFieldValue(
                            rawOutput,
                            reflectionContext.getCopyingChainingOutputClass(),
                            "serializer");

            return (Output<StreamRecord<IterationRecord<?>>>)
                    ReflectionUtils.createInstance(
                            reflectionContext.getCopyingChainingOutputClass(),
                            Arrays.asList(
                                    Input.class,
                                    TypeSerializer.class,
                                    OperatorMetricGroup.class,
                                    OutputTag.class),
                            new Object[] {
                                wrappedOperator,
                                miniBatchRecordSerializer.getIterationRecordSerializer(),
                                ((StreamOperator) wrapperOperator).getMetricGroup(),
                                reflectionContext.getChainingOutputTag(rawOutput)
                            });
        } else {
            return (Output<StreamRecord<IterationRecord<?>>>)
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
