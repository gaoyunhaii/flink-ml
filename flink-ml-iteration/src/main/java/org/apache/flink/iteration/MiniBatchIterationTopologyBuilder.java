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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.compile.DraftExecutionEnvironment;
import org.apache.flink.iteration.minibatch.MiniBatchOperatorWrapper;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.iteration.minibatch.MiniBatchRecordTypeInfo;
import org.apache.flink.iteration.minibatch.operator.MiniBatchHeadOperatorWrapperFactory;
import org.apache.flink.iteration.minibatch.operator.MiniBatchInputOperator;
import org.apache.flink.iteration.minibatch.operator.MiniBatchOutputOperator;
import org.apache.flink.iteration.minibatch.operator.MiniBatchReplayOperatorWrapperFactory;
import org.apache.flink.iteration.minibatch.operator.MiniBatchTailOperator;
import org.apache.flink.iteration.operator.HeadOperator;
import org.apache.flink.iteration.operator.HeadOperatorFactory;
import org.apache.flink.iteration.operator.OperatorWrapper;
import org.apache.flink.iteration.operator.ReplayOperator;
import org.apache.flink.iteration.typeinfo.IterationRecordTypeInfo;
import org.apache.flink.statefun.flink.core.feedback.MiniBatchFeedbackChannelProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/** mini-batched builder. */
public class MiniBatchIterationTopologyBuilder extends IterationTopologyBuilder {

    private final int miniBatchRecords;

    public MiniBatchIterationTopologyBuilder(int miniBatchRecords) {
        this.miniBatchRecords = miniBatchRecords;
    }

    @Override
    protected DataStreamList addInputs(DataStreamList dataStreams) {
        return new DataStreamList(
                map(
                        dataStreams,
                        dataStream ->
                                dataStream
                                        .transform(
                                                "input-" + dataStream.getTransformation().getName(),
                                                new MiniBatchRecordTypeInfo<>(
                                                        new IterationRecordTypeInfo<>(
                                                                dataStream.getType(), true)),
                                                new MiniBatchInputOperator(miniBatchRecords))
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
                                ((SingleOutputStreamOperator<MiniBatchRecord<?>>) dataStream)
                                        .transform(
                                                "head-"
                                                        + variableStreams
                                                                .get(index)
                                                                .getTransformation()
                                                                .getName(),
                                                (MiniBatchRecordTypeInfo) dataStream.getType(),
                                                new MiniBatchHeadOperatorWrapperFactory(
                                                        new HeadOperatorFactory(
                                                                iterationId,
                                                                startHeaderIndex + index,
                                                                isCriteriaStream,
                                                                totalInitVariableParallelism,
                                                                new MiniBatchFeedbackChannelProvider()),
                                                        miniBatchRecords))
                                        .setParallelism(dataStream.getParallelism())));
    }

    @Override
    protected DataStreamList addTails(
            DataStreamList dataStreams, IterationID iterationId, int startIndex) {
        return new DataStreamList(
                map(
                        dataStreams,
                        (index, dataStream) ->
                                ((DataStream<MiniBatchRecord<?>>) dataStream)
                                        .transform(
                                                "tail-" + dataStream.getTransformation().getName(),
                                                new MiniBatchRecordTypeInfo<>(
                                                        new IterationRecordTypeInfo(
                                                                dataStream.getType(), true)),
                                                new MiniBatchTailOperator(
                                                        iterationId, startIndex + index))
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
                                    ((SingleOutputStreamOperator<MiniBatchRecord<?>>)
                                                    firstHeadStream)
                                            .getSideOutput(
                                                    new OutputTag<>(
                                                            HeadOperator.ALIGN_NOTIFY_OUTPUT_TAG
                                                                    .getId(),
                                                            new MiniBatchRecordTypeInfo<>(
                                                                    (IterationRecordTypeInfo)
                                                                            HeadOperator
                                                                                    .ALIGN_NOTIFY_OUTPUT_TAG
                                                                                    .getTypeInfo())))
                                            .broadcast())
                            .transform(
                                    "Replayer-"
                                            + originalDataStreams
                                                    .get(i)
                                                    .getTransformation()
                                                    .getName(),
                                    dataStreamInputs.get(i).getType(),
                                    new MiniBatchReplayOperatorWrapperFactory(
                                            new ReplayOperator<>(), miniBatchRecords))
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
                            MiniBatchRecordTypeInfo<?> inputType =
                                    (MiniBatchRecordTypeInfo<?>) dataStream.getType();
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
                                            inputType
                                                    .getIterationRecordTypeInfo()
                                                    .getInnerTypeInfo(),
                                            new MiniBatchOutputOperator())
                                    .setParallelism(dataStream.getParallelism());
                        }));
    }

    @Override
    protected DraftExecutionEnvironment createDraftExecutionEnvironment(
            StreamExecutionEnvironment env,
            OperatorWrapper<?, IterationRecord<?>> initialOperatorWrapper) {
        return new DraftExecutionEnvironment(env, initialOperatorWrapper) {
            @Override
            public OperatorWrapper<?, ?> setCurrentWrapper(OperatorWrapper<?, ?> newWrapper) {
                OperatorWrapper<?, ?> existing =
                        super.setCurrentWrapper(
                                new MiniBatchOperatorWrapper<>(
                                        (OperatorWrapper) newWrapper, miniBatchRecords));

                if (existing == null) {
                    return null;
                }

                checkState(existing instanceof MiniBatchOperatorWrapper);
                return ((MiniBatchOperatorWrapper) existing).getIterationWrapper();
            }
        };
    }

    @Override
    protected TypeInformation<?> unwrapTypeInfo(TypeInformation<?> wrappedTypeInfo) {
        // It should always has the IterationRecordTypeInfo
        checkState(
                wrappedTypeInfo instanceof MiniBatchRecordTypeInfo,
                "The termination criteria should always return IterationRecord.");
        return ((MiniBatchRecordTypeInfo<?>) wrappedTypeInfo)
                .getIterationRecordTypeInfo()
                .getInnerTypeInfo();
    }

    @Override
    protected void setCriteriaParallelism(DataStreamList headStreams, int criteriaParallelism) {
        map(
                headStreams,
                dataStream -> {
                    (((MiniBatchHeadOperatorWrapperFactory)
                                    ((OneInputTransformation) dataStream.getTransformation())
                                            .getOperatorFactory()))
                            .setCriteriaStreamParallelism(criteriaParallelism);
                    return null;
                });
    }
}
