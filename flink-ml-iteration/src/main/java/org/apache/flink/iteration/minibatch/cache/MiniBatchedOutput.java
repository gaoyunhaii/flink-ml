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
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.broadcast.OutputReflectionContext;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.iteration.minibatch.proxy.MiniBatchCalculatedStreamPartitioner;
import org.apache.flink.iteration.minibatch.proxy.MiniBatchProxyStreamPartitioner;
import org.apache.flink.iteration.utils.ReflectionUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.network.api.writer.BroadcastRecordWriter;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Cache a mini-batch of records. */
public class MiniBatchedOutput<T>
        implements Output<StreamRecord<IterationRecord<T>>>, BroadcastOutput<IterationRecord<T>> {

    private final Output<StreamRecord<MiniBatchRecord<T>>> output;

    private final List<MiniBatchCache> nonTaggedMiniBatchCaches;

    private final Map<String, MiniBatchCache> taggedMiniBatchCaches;

    private final BroadcastOutput<MiniBatchRecord<T>> broadcastOutput;

    public MiniBatchedOutput(
            Output<StreamRecord<MiniBatchRecord<T>>> output,
            int miniBatchRecords,
            Counter outputCounter) {
        this.output = output;

        OutputReflectionContext reflectionContext = new OutputReflectionContext();

        List<OutputContext> contexts = new ArrayList<>();
        if (reflectionContext.isChainingOutput(output)) {
            contexts.add(buildChainedOutputContext(output, reflectionContext));
        } else if (reflectionContext.isRecordWriterOutput(output)) {
            contexts.add(buildRecordWriterOutputContext(output, reflectionContext));
        } else if (reflectionContext.isBroadcastingOutput(output)) {
            Output[] innerOutputs = reflectionContext.getBroadcastingInternalOutputs(output);
            for (int i = 0; i < innerOutputs.length; ++i) {
                if (reflectionContext.isChainingOutput(innerOutputs[i])) {
                    contexts.add(buildChainedOutputContext(innerOutputs[i], reflectionContext));
                } else if (reflectionContext.isRecordWriterOutput(innerOutputs[i])) {
                    contexts.add(
                            buildRecordWriterOutputContext(innerOutputs[i], reflectionContext));
                } else {
                    throw new RuntimeException(
                            "Unknown output type: " + innerOutputs[i].getClass());
                }
            }
        } else {
            throw new RuntimeException("Unknown output type: " + output.getClass());
        }

        nonTaggedMiniBatchCaches = new ArrayList<>();
        taggedMiniBatchCaches = new HashMap<>();
        for (int i = 0; i < contexts.size(); ++i) {
            MiniBatchCache cache;
            if (!contexts.get(i).isCalculated) {
                cache =
                        new SingleMiniBatchCache(
                                contexts.get(i).output,
                                contexts.get(i).outputTag,
                                miniBatchRecords,
                                -1);
            } else {
                cache =
                        new MultiMiniBatchCache(
                                contexts.get(i).output,
                                contexts.get(i).outputTag,
                                contexts.get(i).partitioner,
                                contexts.get(i).numberOfChannels,
                                miniBatchRecords);
            }

            if (contexts.get(i).outputTag == null) {
                nonTaggedMiniBatchCaches.add(cache);
            } else {
                taggedMiniBatchCaches.put(contexts.get(i).outputTag.getId(), cache);
            }
        }

        this.broadcastOutput = BroadcastOutputFactory.createBroadcastOutput(output, outputCounter);
    }

    private OutputContext buildChainedOutputContext(
            Output<?> output, OutputReflectionContext reflectionContext) {
        return new OutputContext(
                reflectionContext.getChainingOutputTag(output), null, 1, output, false);
    }

    private OutputContext buildRecordWriterOutputContext(
            Output<?> output, OutputReflectionContext reflectionContext) {
        RecordWriter recordWriter =
                ReflectionUtils.getFieldValue(output, RecordWriterOutput.class, "recordWriter");
        if (recordWriter instanceof BroadcastRecordWriter) {
            return new OutputContext(
                    reflectionContext.getRecordWriterOutputTag(output), null, 1, output, false);
        }

        StreamPartitioner channelSelector =
                ReflectionUtils.getFieldValue(
                        recordWriter, ChannelSelectorRecordWriter.class, "channelSelector");

        if (channelSelector instanceof MiniBatchProxyStreamPartitioner) {
            return new OutputContext(
                    reflectionContext.getRecordWriterOutputTag(output),
                    ((MiniBatchProxyStreamPartitioner) channelSelector)
                            .getWrappedStreamPartitioner(),
                    ((MiniBatchProxyStreamPartitioner) channelSelector).getNumberOfChannels(),
                    output,
                    false);
        } else if (channelSelector instanceof MiniBatchCalculatedStreamPartitioner) {
            return new OutputContext(
                    reflectionContext.getRecordWriterOutputTag(output),
                    ((MiniBatchCalculatedStreamPartitioner) channelSelector)
                            .getWrappedStreamPartitioner(),
                    ((MiniBatchCalculatedStreamPartitioner) channelSelector).getNumberOfChannels(),
                    output,
                    true);
        } else if (channelSelector instanceof ForwardPartitioner
                || channelSelector instanceof RescalePartitioner
                || channelSelector instanceof RebalancePartitioner) {
            return new OutputContext(
                    reflectionContext.getRecordWriterOutputTag(output),
                    channelSelector,
                    1,
                    output,
                    false);
        } else {
            throw new RuntimeException(
                    "unsupported channel selector: " + channelSelector.getClass());
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

    @Override
    public void broadcastEmit(StreamRecord<IterationRecord<T>> record) throws IOException {
        IterationRecord iterationRecord = record.getValue();

        // flush all
        for (MiniBatchCache cache : nonTaggedMiniBatchCaches) {
            cache.flush();
        }

        for (Map.Entry<String, MiniBatchCache> entry : taggedMiniBatchCaches.entrySet()) {
            entry.getValue().flush();
        }

        // Directly output them
        MiniBatchRecord<T> tmp = new MiniBatchRecord<>();
        tmp.addRecord(iterationRecord, record.hasTimestamp() ? record.getTimestamp() : null);
        broadcastOutput.broadcastEmit(new StreamRecord<>(tmp));
    }

    private static class OutputContext {
        final OutputTag outputTag;
        final StreamPartitioner partitioner;
        final int numberOfChannels;
        final Output output;
        final boolean isCalculated;

        public OutputContext(
                OutputTag outputTag,
                StreamPartitioner partitioner,
                int numberOfChannels,
                Output output,
                boolean isCalculated) {
            checkState(numberOfChannels >= 1, "Number of channels not set");
            this.outputTag = outputTag;
            this.partitioner = partitioner;
            this.numberOfChannels = numberOfChannels;
            this.output = output;
            this.isCalculated = isCalculated;
        }
    }
}
