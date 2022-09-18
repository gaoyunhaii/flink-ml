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
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.statefun.flink.core.feedback.FeedbackChannel;
import org.apache.flink.statefun.flink.core.feedback.RecordwiseFeedbackChannelProvider;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import java.util.Objects;
import java.util.function.Consumer;

public class MiniBatchTailOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<MiniBatchRecord<?>, Void> {

    private final IterationID iterationId;

    private final int feedbackIndex;

    private transient MailboxExecutor mailboxExecutor;

    /** We distinguish how the record is processed according to if objectReuse is enabled. */
    private transient Consumer<MiniBatchRecord<?>> recordConsumer;

    private transient FeedbackChannel<MiniBatchRecord<?>> channel;

    public MiniBatchTailOperator(IterationID iterationId, int feedbackIndex) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.feedbackIndex = feedbackIndex;
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

        this.recordConsumer = this::processIfObjectReuseEnabled;
    }

    @Override
    public void processElement(StreamRecord<MiniBatchRecord<?>> streamRecord) {
        recordConsumer.accept(streamRecord.getValue());
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
}
