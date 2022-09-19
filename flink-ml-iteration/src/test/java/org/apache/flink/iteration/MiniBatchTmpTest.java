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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class MiniBatchTmpTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);
        DataStream<Integer> source =
                env.fromElements(1)
                        .flatMap(
                                new FlatMapFunction<Integer, Integer>() {
                                    @Override
                                    public void flatMap(
                                            Integer integer, Collector<Integer> collector)
                                            throws Exception {
                                        for (int i = 0; i < 23; ++i) {
                                            collector.collect(i);
                                        }
                                    }
                                });

        DataStreamList result =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(source),
                        ReplayableDataStreamList.notReplay(),
                        IterationConfig.newBuilder().setMiniBatchRecords(10).build(),
                        ((variableStreams, dataStreams) -> {
                            DataStream<Integer> input = variableStreams.get(0);
                            SingleOutputStreamOperator<Integer> processed =
                                    input.process(new MyMapFunction());

                            //                            SingleOutputStreamOperator<Integer>
                            // processed =
                            //                                    input.keyBy(i -> i %
                            // 2).process(new MyMapFunction2());

                            return new IterationBodyResult(
                                    DataStreamList.of(processed),
                                    DataStreamList.of(
                                            processed.getSideOutput(MyMapFunction.FINAL_OUTPUT)));
                        }));

        result.<Integer>get(0)
                .addSink(
                        new SinkFunction<Integer>() {
                            @Override
                            public void invoke(Integer value, Context context) throws Exception {
                                System.out.println(value);
                            }
                        });
        System.out.println(env.getStreamGraph(false).getStreamingPlanAsJSON());
        env.execute();
    }

    private static class MyMapFunction extends ProcessFunction<Integer, Integer>
            implements IterationListener<Integer> {

        public static final OutputTag<Integer> FINAL_OUTPUT = new OutputTag<Integer>("final") {};

        private final List<Integer> integers = new ArrayList<>();

        @Override
        public void processElement(
                Integer integer,
                ProcessFunction<Integer, Integer>.Context context,
                Collector<Integer> collector)
                throws Exception {
            integers.add(integer);
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, IterationListener.Context context, Collector<Integer> collector)
                throws Exception {
            if (epochWatermark < 10) {
                integers.forEach(collector::collect);
                integers.clear();
            }
        }

        @Override
        public void onIterationTerminated(
                IterationListener.Context context, Collector<Integer> collector) throws Exception {
            integers.forEach(i -> context.output(FINAL_OUTPUT, i));
        }
    }

    private static class MyMapFunction2 extends KeyedProcessFunction<Integer, Integer, Integer>
            implements IterationListener<Integer> {

        public static final OutputTag<Integer> FINAL_OUTPUT = new OutputTag<Integer>("final") {};

        private final List<Integer> integers = new ArrayList<>();

        private ValueState<Integer> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            sum =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>("sum", BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public void processElement(
                Integer integer,
                KeyedProcessFunction<Integer, Integer, Integer>.Context context,
                Collector<Integer> collector)
                throws Exception {
            integers.add(integer);
            sum.update(sum.value() == null ? integer : integer + sum.value());
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, IterationListener.Context context, Collector<Integer> collector)
                throws Exception {
            if (epochWatermark < 10) {
                integers.forEach(collector::collect);
                integers.clear();
            }
        }

        @Override
        public void onIterationTerminated(
                IterationListener.Context context, Collector<Integer> collector) throws Exception {
            integers.forEach(i -> context.output(FINAL_OUTPUT, i));
        }
    }
}
