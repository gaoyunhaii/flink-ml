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

package org.apache.flink.iteration.operator;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.checkpoint.Checkpoints;
import org.apache.flink.iteration.checkpoint.CheckpointsBroker;
import org.apache.flink.iteration.config.IterationOptions;
import org.apache.flink.iteration.feedback.FeedbackConfiguration;
import org.apache.flink.iteration.feedback.SerializedFeedbackChannel;
import org.apache.flink.statefun.flink.core.feedback.FeedbackChannelBroker;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.RecordBasedFeedbackChannel;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.IOUtils;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * The tail operators is attached after each feedback operator to increment the round of each
 * record.
 */
public class TailOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<IterationRecord<?>, Void> {

    private final IterationID iterationId;

    private final int feedbackIndex;

    private transient MailboxExecutor mailboxExecutor;

    /** We distinguish how the record is processed according to if objectReuse is enabled. */
    private transient Consumer<IterationRecord<?>> recordConsumer;

    private transient RecordBasedFeedbackChannel<IterationRecord<?>> channel;

    public TailOperator(IterationID iterationId, int feedbackIndex) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.feedbackIndex = feedbackIndex;
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

        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int attemptNum = getRuntimeContext().getAttemptNumber();

        FeedbackKey<IterationRecord<?>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, feedbackIndex);
        SubtaskFeedbackKey<IterationRecord<?>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);

        FeedbackChannelBroker broker = FeedbackChannelBroker.get();

        Configuration configuration =
                getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration();
        IterationOptions.FeedbackType feedbackType =
                configuration.get(IterationOptions.FEEDBACK_CHANNEL_TYPE);

        Executor executor =
                (Runnable runnable) -> mailboxExecutor.execute(runnable::run, "Tail feedback");

        if (feedbackType.equals(IterationOptions.FeedbackType.RECORD)) {
            this.channel = broker.getChannel(key, RecordBasedFeedbackChannel::new);
            channel.registerProducer(executor);
        } else {
            this.channel =
                    broker.getChannel(
                            key,
                            (ignored) ->
                                    new SerializedFeedbackChannel<>(
                                            new FeedbackConfiguration(
                                                    configuration,
                                                    getContainingTask()
                                                            .getEnvironment()
                                                            .getIOManager()
                                                            .getSpillingDirectoriesPaths()),
                                            config.getTypeSerializerIn(
                                                    0, getClass().getClassLoader())));
            channel.registerProducer(executor);
        }

        this.recordConsumer = this::processIfObjectReuseEnabled;
    }

    @Override
    public void processElement(StreamRecord<IterationRecord<?>> streamRecord) {
        recordConsumer.accept(streamRecord.getValue());
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        channel.put(IterationRecord.newBarrier(checkpointId));
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

    private void processIfObjectReuseEnabled(IterationRecord<?> record) {
        // Since the record would be reused, we have to clone a new one
        IterationRecord<?> cloned = record.clone();
        cloned.incrementEpoch();
        channel.put(cloned);
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(channel);
        super.close();
    }
}
