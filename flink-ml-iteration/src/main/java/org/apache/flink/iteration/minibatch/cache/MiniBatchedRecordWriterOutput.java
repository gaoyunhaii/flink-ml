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
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.flink.util.Preconditions.checkState;

/** Cache a mini-batch of records. */
public class MiniBatchedRecordWriterOutput<T>
        implements Output<StreamRecord<IterationRecord<T>>>, BroadcastOutput<IterationRecord<T>> {

    private final Output<StreamRecord<MiniBatchRecord<T>>> output;

    private final BroadcastOutput<MiniBatchRecord<T>> broadcastOutput;

    private final OutputTag<?> outputTag;

    private final MiniBatchCache miniBatchCache;

    public MiniBatchedRecordWriterOutput(
            OutputReflectionContext reflectionContext,
            Output<StreamRecord<MiniBatchRecord<T>>> output,
            int miniBatchRecords,
            Counter numRecordsOut,
            StreamConfig streamConfig) {
        this.output = output;
        this.broadcastOutput = BroadcastOutputFactory.createBroadcastOutput(output, numRecordsOut);

        this.outputTag = reflectionContext.getRecordWriterOutputTag(output);

        OutputContext outputContext = buildRecordWriterOutputContext(output, reflectionContext);
        // Let's build the mini-batch from the outputContext

        // Let's now have a dedicated collector to skip other things

        SimplifiedCollector collector =
                new SimplifiedCollector(
                        ReflectionUtils.getFieldValue(
                                output, RecordWriterOutput.class, "serializationDelegate"),
                        ReflectionUtils.getFieldValue(
                                output, RecordWriterOutput.class, "recordWriter"));

        if (!outputContext.isCalculated) {
            this.miniBatchCache =
                    new SingleMiniBatchCache(
                            collector,
                            outputContext.outputTag,
                            miniBatchRecords,
                            -1,
                            outputContext.outputTag == null
                                    ? streamConfig.getTypeSerializerOut(getClass().getClassLoader())
                                    : streamConfig.getTypeSerializerSideOut(
                                            outputContext.outputTag, getClass().getClassLoader()));
        } else {
            this.miniBatchCache =
                    new MultiMiniBatchCache(
                            collector,
                            outputContext.outputTag,
                            outputContext.partitioner,
                            outputContext.numberOfChannels,
                            miniBatchRecords,
                            outputContext.outputTag == null
                                    ? streamConfig.getTypeSerializerOut(getClass().getClassLoader())
                                    : streamConfig.getTypeSerializerSideOut(
                                            outputContext.outputTag, getClass().getClassLoader()));
        }
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
        if (outputTag == null) {
            if (record.getValue().getType() == IterationRecord.Type.RECORD) {
                miniBatchCache.collect(
                        record.getValue(), record.hasTimestamp() ? record.getTimestamp() : null);
            } else {
                miniBatchCache.flush();
                miniBatchCache.collect(record.getValue(), null);
                miniBatchCache.flush();
            }
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        IterationRecord<?> iterationRecord = (IterationRecord<?>) record.getValue();
        if (OutputTag.isResponsibleFor(this.outputTag, outputTag)) {
            if (iterationRecord.getType() == IterationRecord.Type.RECORD) {
                miniBatchCache.collect(
                        iterationRecord, record.hasTimestamp() ? record.getTimestamp() : null);
            } else {
                miniBatchCache.flush();
                miniBatchCache.collect(iterationRecord, null);
                miniBatchCache.flush();
            }
        }
    }

    @Override
    public void close() {
        output.close();
    }

    @Override
    public void broadcastEmit(StreamRecord<IterationRecord<T>> record) throws IOException {
        miniBatchCache.flush();

        // Let's directly emit the record.
        MiniBatchRecord<T> tmp = new MiniBatchRecord<>();
        tmp.addRecord(record.getValue(), record.hasTimestamp() ? record.getTimestamp() : null);
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

    private static class SimplifiedCollector
            implements Collector<StreamRecord<MiniBatchRecord<?>>> {

        private final SerializationDelegate<StreamElement> serializationDelegate;

        private final RecordWriter recordWriter;

        public SimplifiedCollector(
                SerializationDelegate<StreamElement> serializationDelegate,
                RecordWriter recordWriter) {
            this.serializationDelegate = serializationDelegate;
            this.recordWriter = recordWriter;
        }

        @Override
        public void collect(StreamRecord<MiniBatchRecord<?>> miniBatchRecordStreamRecord) {
            serializationDelegate.setInstance(miniBatchRecordStreamRecord);

            try {
                recordWriter.emit(serializationDelegate);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

        @Override
        public void close() {}
    }
}
