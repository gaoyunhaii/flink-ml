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

package org.apache.flink.iteration.minibatch.operator;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.checkpoint.Checkpoints;
import org.apache.flink.iteration.checkpoint.CheckpointsBroker;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.iteration.minibatch.cache.SingleMiniBatchCache;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.statefun.flink.core.feedback.FeedbackChannel;
import org.apache.flink.statefun.flink.core.feedback.RecordwiseFeedbackChannelProvider;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import java.util.Objects;

public class MiniBatchTailOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<MiniBatchRecord<?>, Void>, WrapperOperator {

    private final IterationID iterationId;

    private final int feedbackIndex;

    private final int miniBatchRecords;

    private transient MailboxExecutor mailboxExecutor;

    /** We distinguish how the record is processed according to if objectReuse is enabled. */
    private transient FeedbackChannel<MiniBatchRecord<?>> channel;

    public MiniBatchTailOperator(IterationID iterationId, int feedbackIndex, int miniBatchRecords) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.feedbackIndex = feedbackIndex;
        this.miniBatchRecords = miniBatchRecords;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Void>> output) {
        super.setup(containingTask, config, output);
        mailboxExecutor = containingTask.getMailboxExecutorFactory().createExecutor(0);
    }

    @Override
    public void open() throws Exception {
        super.open();

        channel =
                new RecordwiseFeedbackChannelProvider()
                        .getProducerChannel(
                                getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration(),
                                getContainingTask()
                                        .getEnvironment()
                                        .getIOManager()
                                        .getSpillingDirectoriesPaths(),
                                config,
                                iterationId,
                                feedbackIndex,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getAttemptNumber(),
                                runnable ->
                                        mailboxExecutor.execute(runnable::run, "Tail feedback"));
    }

    @Override
    public void processElement(StreamRecord<MiniBatchRecord<?>> streamRecord) {
        processIfObjectReuseEnabled(streamRecord.getValue());
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);

        MiniBatchRecord<?> miniBatchRecord = new MiniBatchRecord<>();
        miniBatchRecord.getRecords().add(IterationRecord.newBarrier(checkpointId));
        miniBatchRecord.getTimestamps().add(null);
        channel.put(miniBatchRecord);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        // TODO: Unfortunately, we have to rely on the tail operator to help
        // abort the checkpoint since the task thread of the head operator
        // might get blocked due to not be able to close the raw state files.
        // We would try to fix it in the Flink side in the future.
        SubtaskFeedbackKey<?> key =
                OperatorUtils.createFeedbackKey(iterationId, feedbackIndex)
                        .withSubTaskIndex(
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getAttemptNumber());
        Checkpoints<?> checkpoints = CheckpointsBroker.get().getCheckpoints(key);
        if (checkpoints != null) {
            checkpoints.abort(checkpointId);
        }
    }

    private void processIfObjectReuseEnabled(MiniBatchRecord<?> record) {
        // Since the record would be reused, we have to clone a new one
        record.getRecords().forEach(IterationRecord::incrementEpoch);
        try {
            channel.put(record);
        } catch (Exception exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(channel);
        super.close();
    }

    @Override
    public Input<IterationRecord<?>> getWrappedOperator() {
        return new ProxyTailOperator(miniBatchRecords);
    }

    public class ProxyTailOperator implements Input<IterationRecord<?>> {

        private final SingleMiniBatchCache cache;

        public ProxyTailOperator(int miniBatchRecords) {
            this.cache =
                    new SingleMiniBatchCache(
                            new Collector<StreamRecord<MiniBatchRecord<?>>>() {
                                @Override
                                public void collect(
                                        StreamRecord<MiniBatchRecord<?>>
                                                miniBatchRecordStreamRecord) {
                                    MiniBatchTailOperator.this.processIfObjectReuseEnabled(
                                            miniBatchRecordStreamRecord.getValue());
                                }

                                @Override
                                public void close() {}
                            },
                            null,
                            miniBatchRecords,
                            -1,
                            config.getTypeSerializerOut(getClass().getClassLoader()));
        }

        @Override
        public void processElement(StreamRecord<IterationRecord<?>> streamRecord) throws Exception {
            if (streamRecord.getValue().getType() == IterationRecord.Type.RECORD) {
                cache.collect(
                        streamRecord.getValue(),
                        streamRecord.hasTimestamp() ? streamRecord.getTimestamp() : null);
            } else {
                cache.flush();
                cache.collect(
                        streamRecord.getValue(),
                        streamRecord.hasTimestamp() ? streamRecord.getTimestamp() : null);
                cache.flush();
            }
        }

        @Override
        public void processWatermark(Watermark watermark) throws Exception {
            MiniBatchTailOperator.this.processWatermark(watermark);
        }

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            MiniBatchTailOperator.this.processWatermarkStatus(watermarkStatus);
        }

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            MiniBatchTailOperator.this.processLatencyMarker(latencyMarker);
        }

        @Override
        public void setKeyContextElement(StreamRecord<IterationRecord<?>> streamRecord)
                throws Exception {}
    }
}
