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

import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.broadcast.OutputReflectionContext;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.iteration.minibatch.proxy.MiniBatchCalculatedStreamPartitioner;
import org.apache.flink.iteration.proxy.ProxyOutput;
import org.apache.flink.iteration.utils.ReflectionUtils;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelectorRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Cache a mini-batch of records. */
public class MiniBatchedOutput<T> implements Output<StreamRecord<IterationRecord<T>>> {

    private final Output<StreamRecord<MiniBatchRecord<T>>> output;

    private final List<MiniBatchCache> nonTaggedMiniBatchCaches;

    private final Map<String, MiniBatchCache> taggedMiniBatchCaches;

    public MiniBatchedOutput(
            Output<StreamRecord<MiniBatchRecord<T>>> output, int miniBatchRecords) {
        this.output = output;

        // Now it's time to build up the cache structure for each wrapped output
        checkState(output instanceof ProxyOutput);
        Output<?> recordLevelOutput =
                ReflectionUtils.getFieldValue(output, ProxyOutput.class, "output");
        OutputReflectionContext reflectionContext = new OutputReflectionContext();
        Output[] innerOutputs = reflectionContext.getBroadcastingInternalOutputs(recordLevelOutput);

        // Now let's acquire the stream partitioner for each output.
        OutputTag[] tags = new OutputTag[innerOutputs.length];
        StreamPartitioner[] partitioners = new StreamPartitioner[innerOutputs.length];
        int[] numberOfChannels = new int[innerOutputs.length];

        for (int i = 0; i < innerOutputs.length; ++i) {
            if (reflectionContext.isChainingOutput(innerOutputs[i])) {
                tags[i] = reflectionContext.getChainingOutputTag(innerOutputs[i]);
                partitioners[i] = null;
            } else if (reflectionContext.isRecordWriterOutput(innerOutputs[i])) {
                tags[i] = reflectionContext.getRecordWriterOutputTag(innerOutputs[i]);
                RecordWriter recordWriter =
                        ReflectionUtils.getFieldValue(
                                innerOutputs[i], RecordWriterOutput.class, "recordWriter");
                MiniBatchCalculatedStreamPartitioner channelSelector =
                        ReflectionUtils.getFieldValue(
                                recordWriter, ChannelSelectorRecordWriter.class, "channelSelector");
                partitioners[i] = channelSelector.getWrappedStreamPartitioner();
                numberOfChannels[i] = channelSelector.getNumberOfChannels();
                checkState(numberOfChannels[i] >= 1, "The number of channels is not set");
            } else {
                throw new RuntimeException("Unknown output type: " + innerOutputs[i].getClass());
            }
        }

        nonTaggedMiniBatchCaches = new ArrayList<>();
        taggedMiniBatchCaches = new HashMap<>();
        for (int i = 0; i < innerOutputs.length; ++i) {
            MiniBatchCache cache;
            if (partitioners[i] == null
                    || partitioners[i] instanceof ForwardPartitioner
                    || partitioners[i] instanceof RescalePartitioner
                    || partitioners[i] instanceof RebalancePartitioner) {
                cache = new SingleMiniBatchCache(innerOutputs[i], miniBatchRecords, -1);
            } else {
                cache =
                        new MultiMiniBatchCache(
                                innerOutputs[i],
                                partitioners[i],
                                numberOfChannels[i],
                                miniBatchRecords);
            }

            if (tags[i] == null) {
                nonTaggedMiniBatchCaches.add(cache);
            } else {
                taggedMiniBatchCaches.put(tags[i].getId(), cache);
            }
        }
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        // For now, we only supports the MAX_WATERMARK separately for each operator.
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        output.emitWatermarkStatus(watermarkStatus);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        output.emitLatencyMarker(latencyMarker);
    }

    @Override
    public void collect(StreamRecord<IterationRecord<T>> record) {
        if (record.getValue().getType() == IterationRecord.Type.RECORD) {
            for (MiniBatchCache cache : nonTaggedMiniBatchCaches) {
                cache.collect(
                        record.getValue(), record.hasTimestamp() ? record.getTimestamp() : null);
            }
        } else {
            for (MiniBatchCache cache : nonTaggedMiniBatchCaches) {
                cache.flush();
                cache.collect(record.getValue(), null);
                cache.flush();
            }
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        MiniBatchCache cache = taggedMiniBatchCaches.get(outputTag.getId());
        IterationRecord iterationRecord = (IterationRecord) record.getValue();
        if (iterationRecord.getType() == IterationRecord.Type.RECORD) {
            cache.collect(iterationRecord, record.hasTimestamp() ? record.getTimestamp() : null);
        } else {
            cache.flush();
            cache.collect(iterationRecord, null);
            cache.flush();
        }
    }

    @Override
    public void close() {
        output.close();
    }
}
