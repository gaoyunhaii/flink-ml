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

package org.apache.flink.ml.iteration.operator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.iteration.IterationID;
import org.apache.flink.ml.iteration.IterationRecord;
import org.apache.flink.ml.iteration.broadcast.BroadcastOutput;
import org.apache.flink.ml.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.ml.iteration.checkpoint.Checkpoints;
import org.apache.flink.ml.iteration.config.IterationOptions;
import org.apache.flink.ml.iteration.datacache.nonkeyed.DataCacheSnapshot;
import org.apache.flink.ml.iteration.operator.event.CoordinatorCheckpointEvent;
import org.apache.flink.ml.iteration.operator.event.GloballyAlignedEvent;
import org.apache.flink.ml.iteration.operator.event.SubtaskAlignedEvent;
import org.apache.flink.ml.iteration.typeinfo.IterationRecordTypeInfo;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.statefun.flink.core.feedback.FeedbackChannel;
import org.apache.flink.statefun.flink.core.feedback.FeedbackChannelBroker;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The head operators unions the initialized variable stream and the feedback stream, and
 * synchronize the epoch watermark (round).
 */
public class HeadOperator extends AbstractStreamOperator<IterationRecord<?>>
        implements OneInputStreamOperator<IterationRecord<?>, IterationRecord<?>>,
                FeedbackConsumer<StreamRecord<IterationRecord<?>>>,
                OperatorEventHandler,
                BoundedOneInput {

    public static final OutputTag<IterationRecord<Void>> ALIGN_NOTIFY_OUTPUT_TAG =
            new OutputTag<>("aligned", new IterationRecordTypeInfo<>(BasicTypeInfo.VOID_TYPE_INFO));

    private final IterationID iterationId;

    private final int feedbackIndex;

    private final boolean isCriteriaStream;

    private final OperatorEventGateway operatorEventGateway;

    private final MailboxExecutor mailboxExecutor;

    // ------------- runtime -------------------

    private transient Map<Integer, Long> numFeedbackRecordsPerRound;

    private transient int latestRoundAligned;

    private transient int latestRoundGloballyAligned;

    private transient BroadcastOutput<?> eventBroadcastOutput;

    private transient StreamRecord<IterationRecord<?>> reusable;

    private transient HeadOperatorStatus status;

    // ------------- checkpoints -------------------

    private transient TreeMap<Long, CheckpointAlignmentStatus> checkpointAlignmmentStatuses;

    private transient long latestCheckpointFromCoordinator;

    private transient Checkpoints<IterationRecord<?>> checkpoints;

    private transient ListState<Integer> parallelismState;

    private transient ListState<HeadState> headState;

    public HeadOperator(
            IterationID iterationId,
            int feedbackIndex,
            boolean isCriteriaStream,
            MailboxExecutor mailboxExecutor,
            OperatorEventGateway operatorEventGateway,
            ProcessingTimeService processingTimeService) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.feedbackIndex = feedbackIndex;
        this.isCriteriaStream = isCriteriaStream;
        this.mailboxExecutor = Objects.requireNonNull(mailboxExecutor);
        this.operatorEventGateway = Objects.requireNonNull(operatorEventGateway);

        // Even though this operator does not use the processing
        // time service, AbstractStreamOperator requires this
        // field is non-null, otherwise we get a NullPointerException
        super.processingTimeService = processingTimeService;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<IterationRecord<?>>> output) {
        super.setup(containingTask, config, output);
        eventBroadcastOutput =
                BroadcastOutputFactory.createBroadcastOutput(
                        output, metrics.getIOMetricGroup().getNumRecordsOutCounter());
        status = HeadOperatorStatus.RUNNING;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        reusable = new StreamRecord<>(null);
        this.numFeedbackRecordsPerRound = new HashMap<>();
        this.checkpointAlignmmentStatuses = new TreeMap<>();

        this.latestRoundAligned = -1;
        this.latestRoundGloballyAligned = -1;

        parallelismState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>("parallelism", IntSerializer.INSTANCE));
        Optional<Integer> lastParallelism =
                OperatorStateUtils.getUniqueElement(parallelismState, "parallelism");
        if (lastParallelism.isPresent()
                && lastParallelism.get() != getRuntimeContext().getNumberOfParallelSubtasks()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The Head operator is recovered from a state with parallelism %d, "
                                    + "but the current parallelism is %d, which is not supported",
                            lastParallelism.get(),
                            getRuntimeContext().getNumberOfParallelSubtasks()));
        }

        headState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("headState", HeadState.class));
        Optional<HeadState> lastHeadState =
                OperatorStateUtils.getUniqueElement(headState, "headState");
        if (lastHeadState.isPresent()) {
            numFeedbackRecordsPerRound.putAll(lastHeadState.get().numFeedbackRecordsEachRound);
            latestRoundAligned = lastHeadState.get().getLatestRoundAligned();
            latestRoundGloballyAligned = lastHeadState.get().getLatestRoundGloballyAligned();

            for (int i = latestRoundGloballyAligned + 1; i <= latestRoundAligned; ++i) {
                sendEpochWatermarkToCoordinator(i);
            }
        }

        String basePathConfig =
                getContainingTask()
                        .getEnvironment()
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .get(IterationOptions.DATA_CACHE_PATH);
        if (basePathConfig == null) {
            basePathConfig =
                    getContainingTask()
                            .getEnvironment()
                            .getIOManager()
                            .getSpillingDirectoriesPaths()[0];
        }

        Path basePath = new Path(basePathConfig);

        FileSystem fileSystem = basePath.getFileSystem();
        TypeSerializer<IterationRecord<?>> typeSerializer =
                config.getTypeSerializerOut(getClass().getClassLoader());
        checkpoints =
                new Checkpoints<>(
                        typeSerializer,
                        fileSystem,
                        () -> new Path(basePath, "/checkpoint." + UUID.randomUUID().toString()));

        for (StatePartitionStreamProvider rawStateInput : context.getRawOperatorStateInputs()) {
            DataCacheSnapshot.replay(
                    rawStateInput.getStream(),
                    typeSerializer,
                    fileSystem,
                    (record) -> processRecord(new StreamRecord<>(record)));
        }

        // Here we register a record
        registerFeedbackConsumer(
                (Runnable runnable) -> {
                    if (status != HeadOperatorStatus.TERMINATED) {
                        mailboxExecutor.execute(runnable::run, "Head feedback");
                    }
                });
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);

        // We must wait till we have received the notification from the coordinator.
        CheckpointAlignmentStatus checkpointAlignmentStatus =
                checkpointAlignmmentStatuses.computeIfAbsent(
                        checkpointId, ignored -> new CheckpointAlignmentStatus(true, false));
        while (!checkpointAlignmentStatus.notifiedFromCoordinator) {
            mailboxExecutor.yield();
        }
        checkpointAlignmentStatus.notifiedFromChannels = true;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        parallelismState.clear();
        if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
            parallelismState.update(
                    Collections.singletonList(getRuntimeContext().getNumberOfParallelSubtasks()));
        }

        headState.clear();
        headState.update(
                Collections.singletonList(
                        new HeadState(
                                new HashMap<>(numFeedbackRecordsPerRound),
                                latestRoundAligned,
                                latestRoundGloballyAligned)));
        checkpoints.startLogging(context.getCheckpointId(), context.getRawOperatorStateOutput());

        CheckpointAlignmentStatus checkpointAlignmentStatus =
                checkpointAlignmmentStatuses.remove(context.getCheckpointId());
        checkState(
                checkpointAlignmentStatus.notifiedFromCoordinator
                        && checkpointAlignmentStatus.notifiedFromChannels,
                "Checkpoint " + context.getCheckpointId() + " is not fully aligned");
        for (GloballyAlignedEvent globallyAlignedEvent :
                checkpointAlignmentStatus.pendingGlobalEvents) {
            handleOperatorEvent(globallyAlignedEvent);
        }
    }

    @Override
    public void processElement(StreamRecord<IterationRecord<?>> element) throws Exception {
        processRecord(element);
    }

    @Override
    public void processFeedback(StreamRecord<IterationRecord<?>> iterationRecord) throws Exception {
        switch (iterationRecord.getValue().getType()) {
            case RECORD:
                if (status != HeadOperatorStatus.TERMINATING) {
                    numFeedbackRecordsPerRound.compute(
                            iterationRecord.getValue().getRound(),
                            (round, count) -> count == null ? 1 : count + 1);
                    processRecord(iterationRecord);
                    checkpoints.append(iterationRecord.getValue());
                }
                break;
            case EPOCH_WATERMARK:
                if (status == HeadOperatorStatus.TERMINATING) {
                    // Overflow due to +1
                    checkState(iterationRecord.getValue().getRound() == Integer.MIN_VALUE);
                    status = HeadOperatorStatus.TERMINATED;
                } else {
                    processRecord(iterationRecord);
                }
                break;
            case BARRIER:
                checkpoints.commitCheckpointsUntil(iterationRecord.getValue().getCheckpointId());
                break;
        }
    }

    private void processRecord(StreamRecord<IterationRecord<?>> iterationRecord) {
        switch (iterationRecord.getValue().getType()) {
            case RECORD:
                reusable.replace(iterationRecord.getValue(), iterationRecord.getTimestamp());
                output.collect(reusable);
                break;
            case EPOCH_WATERMARK:
                LOG.info("Head Received epoch watermark {}", iterationRecord.getValue().getRound());
                checkState(iterationRecord.getValue().getRound() > latestRoundAligned);
                latestRoundAligned = iterationRecord.getValue().getRound();
                sendEpochWatermarkToCoordinator(iterationRecord.getValue().getRound());
                break;
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void handleOperatorEvent(OperatorEvent operatorEvent) {
        if (operatorEvent instanceof GloballyAlignedEvent) {
            GloballyAlignedEvent globallyAlignedEvent = (GloballyAlignedEvent) operatorEvent;
            LOG.info("Received global event {}", globallyAlignedEvent);

            // If it is received after operator coordinator notified the latest checkpoint,
            // but the checkpoint is not snapshot, then we have to pending this event
            // until the checkpoint is snapshot.
            CheckpointAlignmentStatus checkpointAlignmentStatus =
                    checkpointAlignmmentStatuses.get(latestCheckpointFromCoordinator);
            if (checkpointAlignmentStatus != null
                    && !checkpointAlignmentStatus.notifiedFromChannels) {
                checkpointAlignmentStatus.pendingGlobalEvents.add(globallyAlignedEvent);
            } else {
                try {
                    checkState(globallyAlignedEvent.getRound() > latestRoundGloballyAligned);
                    latestRoundGloballyAligned = globallyAlignedEvent.getRound();

                    if (globallyAlignedEvent.isTerminated()) {
                        status = HeadOperatorStatus.TERMINATING;
                        reusable.replace(
                                IterationRecord.newEpochWatermark(
                                        Integer.MAX_VALUE,
                                        OperatorUtils.getUniqueSenderId(
                                                getOperatorID(),
                                                getRuntimeContext().getIndexOfThisSubtask())),
                                0);
                    } else {
                        reusable.replace(
                                IterationRecord.newEpochWatermark(
                                        globallyAlignedEvent.getRound(),
                                        OperatorUtils.getUniqueSenderId(
                                                getOperatorID(),
                                                getRuntimeContext().getIndexOfThisSubtask())),
                                0);
                    }

                    eventBroadcastOutput.broadcastEmit((StreamRecord) reusable);

                    // Also notify the listener
                    output.collect(ALIGN_NOTIFY_OUTPUT_TAG, (StreamRecord) reusable);
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                }
            }
        } else if (operatorEvent instanceof CoordinatorCheckpointEvent) {
            CoordinatorCheckpointEvent checkpointEvent = (CoordinatorCheckpointEvent) operatorEvent;
            latestCheckpointFromCoordinator =
                    Math.max(latestCheckpointFromCoordinator, checkpointEvent.getCheckpointId());
            checkpointAlignmmentStatuses.computeIfAbsent(
                                    checkpointEvent.getCheckpointId(),
                                    ignored -> new CheckpointAlignmentStatus(false, true))
                            .notifiedFromCoordinator =
                    true;
        }
    }

    @Override
    public void endInput() throws Exception {
        sendEpochWatermarkToCoordinator(0);
        while (status != HeadOperatorStatus.TERMINATED) {
            mailboxExecutor.yield();
        }
    }

    private void sendEpochWatermarkToCoordinator(int round) {
        operatorEventGateway.sendEventToCoordinator(
                new SubtaskAlignedEvent(
                        round,
                        numFeedbackRecordsPerRound.getOrDefault(round, 0L),
                        isCriteriaStream));
    }

    private void registerFeedbackConsumer(Executor mailboxExecutor) {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int attemptNum = getRuntimeContext().getAttemptNumber();
        FeedbackKey<StreamRecord<IterationRecord<?>>> feedbackKey =
                new FeedbackKey<>(iterationId.toHexString(), feedbackIndex);
        SubtaskFeedbackKey<StreamRecord<IterationRecord<?>>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        FeedbackChannel<StreamRecord<IterationRecord<?>>> channel = broker.getChannel(key);
        OperatorUtils.registerFeedbackConsumer(channel, this, mailboxExecutor);
    }

    @VisibleForTesting
    Map<Integer, Long> getNumFeedbackRecordsPerRound() {
        return numFeedbackRecordsPerRound;
    }

    @VisibleForTesting
    public int getLatestRoundAligned() {
        return latestRoundAligned;
    }

    @VisibleForTesting
    public int getLatestRoundGloballyAligned() {
        return latestRoundGloballyAligned;
    }

    @VisibleForTesting
    public OperatorEventGateway getOperatorEventGateway() {
        return operatorEventGateway;
    }

    @VisibleForTesting
    MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    public HeadOperatorStatus getStatus() {
        return status;
    }

    enum HeadOperatorStatus {
        RUNNING,

        TERMINATING,

        TERMINATED
    }

    private static class CheckpointAlignmentStatus {

        final List<GloballyAlignedEvent> pendingGlobalEvents;

        boolean notifiedFromChannels;

        boolean notifiedFromCoordinator;

        public CheckpointAlignmentStatus(
                boolean notifiedFromChannels, boolean notifiedFromCoordinator) {
            this.pendingGlobalEvents = new ArrayList<>();

            this.notifiedFromChannels = notifiedFromChannels;
            this.notifiedFromCoordinator = notifiedFromCoordinator;
        }
    }

    private static class HeadState {

        private Map<Integer, Long> numFeedbackRecordsEachRound;

        private int latestRoundAligned;

        private int latestRoundGloballyAligned;

        public HeadState() {}

        public HeadState(
                Map<Integer, Long> numFeedbackRecordsEachRound,
                int latestRoundAligned,
                int latestRoundGloballyAligned) {
            this.numFeedbackRecordsEachRound = numFeedbackRecordsEachRound;
            this.latestRoundAligned = latestRoundAligned;
            this.latestRoundGloballyAligned = latestRoundGloballyAligned;
        }

        public Map<Integer, Long> getNumFeedbackRecordsEachRound() {
            return numFeedbackRecordsEachRound;
        }

        public void setNumFeedbackRecordsEachRound(Map<Integer, Long> numFeedbackRecordsEachRound) {
            this.numFeedbackRecordsEachRound = numFeedbackRecordsEachRound;
        }

        public int getLatestRoundAligned() {
            return latestRoundAligned;
        }

        public void setLatestRoundAligned(int latestRoundAligned) {
            this.latestRoundAligned = latestRoundAligned;
        }

        public int getLatestRoundGloballyAligned() {
            return latestRoundGloballyAligned;
        }

        public void setLatestRoundGloballyAligned(int latestRoundGloballyAligned) {
            this.latestRoundGloballyAligned = latestRoundGloballyAligned;
        }
    }
}
