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

package org.apache.flink.ml.common.broadcast.operator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.iteration.config.IterationOptions;
import org.apache.flink.ml.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.ml.iteration.datacache.nonkeyed.DataCacheSnapshot;
import org.apache.flink.ml.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.ml.iteration.datacache.nonkeyed.Segment;
import org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.apache.commons.collections.IteratorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Wrapper for WithBroadcastOneInputStreamOperator. */
public class OneInputBroadcastWrapperOperator<IN, OUT>
        extends AbstractBroadcastWrapperOperator<OUT, OneInputStreamOperator<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    /** used to stored the cached records. It could be local file system or remote file system. */
    private Path basePath;

    FileSystem fileSystem;
    DataCacheWriter<IN> dataCacheWriter;
    DataCacheReader<IN> dataCacheReader;

    List<Segment> segments;

    public OneInputBroadcastWrapperOperator(
            StreamOperatorParameters<OUT> parameters,
            StreamOperatorFactory<OUT> operatorFactory,
            String[] broadcastStreamNames,
            TypeInformation[] inTypes,
            boolean[] isBlocking) {
        super(parameters, operatorFactory, broadcastStreamNames, inTypes, isBlocking);

        basePath =
                new Path(
                        containingTask
                                .getEnvironment()
                                .getTaskManagerInfo()
                                .getConfiguration()
                                .get(IterationOptions.DATA_CACHE_PATH));
        try {
            fileSystem = basePath.getFileSystem();
            dataCacheWriter =
                    new DataCacheWriter<IN>(
                            inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                            fileSystem,
                            () ->
                                    new Path(
                                            basePath.toString()
                                                    + "/"
                                                    + "cache-"
                                                    + parameters
                                                            .getStreamConfig()
                                                            .getOperatorID()
                                                            .toHexString()
                                                    + "-"
                                                    + UUID.randomUUID().toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        segments = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<IN> streamRecord) throws Exception {
        if (isBlocking[0]) {
            if (areBroadcastVariablesReady()) {
                dataCacheWriter.finishAddingRecords();
                segments.addAll(dataCacheWriter.getFinishSegments());
                DataCacheReader<IN> dataCacheReader =
                        new DataCacheReader<>(
                                inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                                fileSystem,
                                segments);
                while (dataCacheReader.hasNext()) {
                    wrappedOperator.processElement(new StreamRecord<>(dataCacheReader.next()));
                }
                wrappedOperator.processElement(streamRecord);

            } else {
                dataCacheWriter.addRecord(streamRecord.getValue());
            }

        } else {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            wrappedOperator.processElement(streamRecord);
        }
    }

    @Override
    public void endInput(int inputId) throws Exception {
        while (!areBroadcastVariablesReady()) {
            mailboxExecutor.yield();
        }
        if (null == dataCacheReader) {
            dataCacheWriter.finishAddingRecords();
            segments.addAll(dataCacheWriter.getFinishSegments());
            dataCacheReader =
                    new DataCacheReader<>(
                            inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                            fileSystem,
                            segments);
        }
        while (dataCacheReader.hasNext()) {
            wrappedOperator.processElement(new StreamRecord<>(dataCacheReader.next()));
        }
        super.endInput(inputId);
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        wrappedOperator.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        wrappedOperator.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker(latencyMarker);
    }

    @Override
    public void setKeyContextElement(StreamRecord<IN> streamRecord) throws Exception {
        wrappedOperator.setKeyContextElement(streamRecord);
    }

    @Override
    public void snapshotState(StateSnapshotContext stateSnapshotContext) throws Exception {
        super.snapshotState(stateSnapshotContext);

        dataCacheWriter.finishAddingRecords();
        segments.addAll(dataCacheWriter.getFinishSegments());
        DataCacheSnapshot dataCacheSnapshot = new DataCacheSnapshot(fileSystem, null, segments);
        OperatorStateCheckpointOutputStream checkpointOutputStream =
                stateSnapshotContext.getRawOperatorStateOutput();
        dataCacheSnapshot.writeTo(checkpointOutputStream);
        dataCacheWriter =
                new DataCacheWriter<IN>(
                        inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                        fileSystem,
                        () ->
                                new Path(
                                        basePath.toString()
                                                + "/"
                                                + "cache-"
                                                + parameters
                                                        .getStreamConfig()
                                                        .getOperatorID()
                                                        .toHexString()
                                                + "-"
                                                + UUID.randomUUID().toString()));
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        super.initializeState(streamTaskStateManager);
        segments.clear();
        int cnt = 0;
        List<StatePartitionStreamProvider> inputs = IteratorUtils.toList(rawStateInputs.iterator());
        for (StatePartitionStreamProvider input : inputs) {
            DataCacheSnapshot dataCacheSnapshot =
                    DataCacheSnapshot.recover(
                            input.getStream(),
                            fileSystem,
                            () ->
                                    new Path(
                                            basePath.toString()
                                                    + "/"
                                                    + "cache-"
                                                    + parameters
                                                            .getStreamConfig()
                                                            .getOperatorID()
                                                            .toHexString()
                                                    + "-"
                                                    + UUID.randomUUID().toString()));
            System.out.println("cz---- restored from input stream id: " + (cnt++));
            segments.addAll(dataCacheSnapshot.getSegments());
        }
    }
}
