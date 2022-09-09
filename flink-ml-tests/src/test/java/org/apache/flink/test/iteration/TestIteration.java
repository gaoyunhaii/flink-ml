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

package org.apache.flink.test.iteration;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.IterationConfig.OperatorLifeCycle;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.junit.Test;

public class TestIteration {

    @Test
    public void testICQ() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Integer> variableStream = env.fromElements(1).name("init_variableStream");
        DataStream<Integer> constantStream = variableStream.map(x -> x).name("init_constantStream");

        IterationConfig config =
                IterationConfig.newBuilder()
                        .setOperatorLifeCycle(OperatorLifeCycle.PER_ROUND)
                        .build();
        IterationBody body = new IcqIterationBody(10);

        DataStream<Integer> loopEnd =
                Iterations.iterateBoundedStreamsUntilTermination(
                                DataStreamList.of(variableStream),
                                ReplayableDataStreamList.notReplay(constantStream),
                                config,
                                body)
                        .get(0);

        loopEnd.print();
        // env.execute();
        System.out.println(env.getStreamGraph().getStreamingPlanAsJSON());
    }

    private static class IcqIterationBody implements IterationBody {

        private final int maxIter;

        public IcqIterationBody(int maxIter) {
            this.maxIter = maxIter;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Integer> variableStream = variableStreams.get(0);

            DataStream<Integer> terminationCriteria =
                    variableStream
                            .flatMap(new TerminateOnMaxIter<>(maxIter))
                            .name("terminationCriteria");

            DataStream<byte[]> input = variableStream.map(x -> new byte[0]).name("input1");

            input = DataStreamUtils.mapPartition(input, new ComputeRichMapPartitionFunction(1));

            input = DataStreamUtils.mapPartition(input, new ComputeRichMapPartitionFunction(2));

            DataStream<Integer> end =
                    DataStreamUtils.mapPartition(
                            input,
                            new MapPartitionFunction<byte[], Integer>() {
                                @Override
                                public void mapPartition(
                                        Iterable<byte[]> values, Collector<Integer> out)
                                        throws Exception {
                                    System.out.println("mapP end-");
                                    out.collect(1);
                                }
                            });
            return new IterationBodyResult(
                    DataStreamList.of(end), DataStreamList.of(end), terminationCriteria);
        }
    }

    private static class ComputeRichMapPartitionFunction
            extends RichMapPartitionFunction<byte[], byte[]> implements IterationListener<byte[]> {

        private final int functionId;
        int superStepNumber;

        public ComputeRichMapPartitionFunction(int functionId) {
            this.functionId = functionId;
        }

        @Override
        public void mapPartition(Iterable<byte[]> values, Collector<byte[]> out) throws Exception {
            System.out.println(
                    "functionId: " + functionId + ", superStepNumber: " + superStepNumber);
        }

        @Override
        public void onEpochWatermarkIncremented(int i, Context context, Collector<byte[]> collector)
                throws Exception {
            this.superStepNumber = i + 1;
        }

        @Override
        public void onIterationTerminated(Context context, Collector<byte[]> collector)
                throws Exception {}
    }
}
