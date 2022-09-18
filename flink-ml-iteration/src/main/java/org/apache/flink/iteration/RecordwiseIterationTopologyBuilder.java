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

import org.apache.flink.iteration.compile.DraftExecutionEnvironment;
import org.apache.flink.iteration.operator.HeadOperator;
import org.apache.flink.iteration.operator.HeadOperatorFactory;
import org.apache.flink.iteration.operator.InputOperator;
import org.apache.flink.iteration.operator.OperatorWrapper;
import org.apache.flink.iteration.operator.OutputOperator;
import org.apache.flink.iteration.operator.ReplayOperator;
import org.apache.flink.iteration.operator.TailOperator;
import org.apache.flink.iteration.typeinfo.IterationRecordTypeInfo;
import org.apache.flink.statefun.flink.core.feedback.RecordwiseFeedbackChannelProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RecordwiseIterationTopologyBuilder extends IterationTopologyBuilder {

    @Override
    protected DataStreamList addInputs(DataStreamList dataStreams) {
        return new DataStreamList(
                map(
                        dataStreams,
                        dataStream ->
                                dataStream
                                        .transform(
                                                "input-" + dataStream.getTransformation().getName(),
                                                new IterationRecordTypeInfo<>(
                                                        dataStream.getType(), true),
                                                new InputOperator())
                                        .setParallelism(dataStream.getParallelism())));
    }

    @Override
    protected DataStreamList addHeads(
            DataStreamList variableStreams,
            DataStreamList inputStreams,
            IterationID iterationId,
            int totalInitVariableParallelism,
            boolean isCriteriaStream,
            int startHeaderIndex) {

        return new DataStreamList(
                map(
                        inputStreams,
                        (index, dataStream) ->
                                ((SingleOutputStreamOperator<IterationRecord<?>>) dataStream)
                                        .transform(
                                                "head-"
                                                        + variableStreams
                                                                .get(index)
                                                                .getTransformation()
                                                                .getName(),
                                                (IterationRecordTypeInfo) dataStream.getType(),
                                                new HeadOperatorFactory(
                                                        iterationId,
                                                        startHeaderIndex + index,
                                                        isCriteriaStream,
                                                        totalInitVariableParallelism,
                                                        new RecordwiseFeedbackChannelProvider()))
                                        .setParallelism(dataStream.getParallelism())));
    }

    @Override
    protected DataStreamList addTails(
            DataStreamList dataStreams, IterationID iterationId, int startIndex) {
        return new DataStreamList(
                map(
                        dataStreams,
                        (index, dataStream) ->
                                ((DataStream<IterationRecord<?>>) dataStream)
                                        .transform(
                                                "tail-" + dataStream.getTransformation().getName(),
                                                new IterationRecordTypeInfo(
                                                        dataStream.getType(), true),
                                                new TailOperator(iterationId, startIndex + index))
                                        .setParallelism(dataStream.getParallelism())));
    }

    protected DataStreamList addReplayer(
            DataStream<?> firstHeadStream,
            DataStreamList originalDataStreams,
            DataStreamList dataStreamInputs,
            Set<Integer> replayedDataStreamIndices) {

        List<DataStream<?>> result = new ArrayList<>(dataStreamInputs.size());
        for (int i = 0; i < dataStreamInputs.size(); ++i) {
            if (!replayedDataStreamIndices.contains(i)) {
                result.add(dataStreamInputs.get(i));
                continue;
            }

            // Notes that the HeadOperator would broadcast the globally aligned events,
            // thus the operator does not require emit to the sideoutput specially.
            DataStream<?> replayedInput =
                    dataStreamInputs
                            .get(i)
                            .connect(
                                    ((SingleOutputStreamOperator<IterationRecord<?>>)
                                                    firstHeadStream)
                                            .getSideOutput(HeadOperator.ALIGN_NOTIFY_OUTPUT_TAG)
                                            .broadcast())
                            .transform(
                                    "Replayer-"
                                            + originalDataStreams
                                                    .get(i)
                                                    .getTransformation()
                                                    .getName(),
                                    dataStreamInputs.get(i).getType(),
                                    (TwoInputStreamOperator) new ReplayOperator<>())
                            .setParallelism(dataStreamInputs.get(i).getParallelism());
            result.add(replayedInput);
        }

        return new DataStreamList(result);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected DataStreamList addOutputs(DataStreamList dataStreams, DataStream tailsUnion) {
        return new DataStreamList(
                map(
                        dataStreams,
                        (index, dataStream) -> {
                            IterationRecordTypeInfo<?> inputType =
                                    (IterationRecordTypeInfo<?>) dataStream.getType();
                            return dataStream
                                    .union(
                                            tailsUnion
                                                    .map(x -> x)
                                                    .name(
                                                            "tail-map-"
                                                                    + dataStream
                                                                            .getTransformation()
                                                                            .getName())
                                                    .returns(inputType)
                                                    .setParallelism(1))
                                    .transform(
                                            "output-" + dataStream.getTransformation().getName(),
                                            inputType.getInnerTypeInfo(),
                                            new OutputOperator())
                                    .setParallelism(dataStream.getParallelism());
                        }));
    }

    @Override
    protected DraftExecutionEnvironment createDraftExecutionEnvironment(
            StreamExecutionEnvironment env,
            OperatorWrapper<?, IterationRecord<?>> initialOperatorWrapper) {
        return new DraftExecutionEnvironment(env, initialOperatorWrapper);
    }
}
