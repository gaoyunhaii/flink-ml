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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.checkpoint.Checkpoints;
import org.apache.flink.iteration.checkpoint.CheckpointsBroker;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheSnapshot;
import org.apache.flink.iteration.operator.event.CoordinatorCheckpointEvent;
import org.apache.flink.iteration.operator.event.GloballyAlignedEvent;
import org.apache.flink.iteration.operator.event.SubtaskAlignedEvent;
import org.apache.flink.iteration.operator.headprocessor.HeadOperatorRecordProcessor;
import org.apache.flink.iteration.operator.headprocessor.HeadOperatorState;
import org.apache.flink.iteration.operator.headprocessor.RegularHeadOperatorRecordProcessor;
import org.apache.flink.iteration.operator.headprocessor.TerminatingHeadOperatorRecordProcessor;
import org.apache.flink.iteration.typeinfo.IterationRecordTypeInfo;
import org.apache.flink.iteration.utils.ReflectionUtils;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionView;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
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

    private transient BroadcastOutput<?> eventBroadcastOutput;

    private transient ContextImpl processorContext;

    // ------------- runtime -------------------

    private HeadOperatorStatus status;

    private HeadOperatorRecordProcessor recordProcessor;

    private HeadOperatorCheckpointAligner checkpointAligner;

    // ------------- states -------------------

    private ListState<Integer> parallelismState;

    private ListState<Integer> statusState;

    private ListState<HeadOperatorState> processorState;

    private Checkpoints<IterationRecord<?>> checkpoints;

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
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        parallelismState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>("parallelism", IntSerializer.INSTANCE));
        OperatorStateUtils.getUniqueElement(parallelismState, "parallelism")
                .ifPresent(
                        oldParallelism ->
                                checkState(
                                        oldParallelism
                                                == getRuntimeContext()
                                                        .getNumberOfParallelSubtasks(),
                                        "The head operator is recovered with parallelism changed from "
                                                + oldParallelism
                                                + " to "
                                                + getRuntimeContext()
                                                        .getNumberOfParallelSubtasks()));
        parallelismState.clear();

        // Initialize the status and the record processor.
        processorContext = new ContextImpl();
        statusState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("status", Integer.class));
        status =
                HeadOperatorStatus.values()[
                        OperatorStateUtils.getUniqueElement(statusState, "status").orElse(0)];
        if (status == HeadOperatorStatus.RUNNING) {
            recordProcessor = new RegularHeadOperatorRecordProcessor(processorContext);
        } else {
            recordProcessor = new TerminatingHeadOperatorRecordProcessor();
        }

        // Recover the process state if exists.
        processorState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "processorState", HeadOperatorState.class));
        OperatorStateUtils.getUniqueElement(processorState, "processorState")
                .ifPresent(
                        headOperatorState ->
                                recordProcessor.initializeState(
                                        headOperatorState, context.getRawOperatorStateInputs()));

        checkpointAligner = new HeadOperatorCheckpointAligner();

        // Initialize the checkpoints
        this.checkpoints =
                new Checkpoints<>(
                        config.getTypeSerializerOut(getClass().getClassLoader()),
                        FileSystem.getLocalFileSystem(),
                        () -> {
                            String[] spillPaths =
                                    getContainingTask()
                                            .getEnvironment()
                                            .getIOManager()
                                            .getSpillingDirectoriesPaths();
                            Random random = new Random();
                            return new Path(
                                    "file://" + spillPaths[random.nextInt(spillPaths.length)],
                                    "checkpoint." + UUID.randomUUID().toString());
                        });
        CheckpointsBroker.get()
                .setCheckpoints(
                        OperatorUtils.<IterationRecord<?>>createFeedbackKey(
                                        iterationId, feedbackIndex)
                                .withSubTaskIndex(
                                        getRuntimeContext().getIndexOfThisSubtask(),
                                        getRuntimeContext().getAttemptNumber()),
                        checkpoints);

        try {
            for (StatePartitionStreamProvider rawStateInput : context.getRawOperatorStateInputs()) {
                DataCacheSnapshot.replay(
                        rawStateInput.getStream(),
                        checkpoints.getTypeSerializer(),
                        checkpoints.getFileSystem(),
                        (record) ->
                                recordProcessor.processFeedbackElement(new StreamRecord<>(record)));
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to replay the records", e);
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

        checkpointAligner.waitTillCoordinatorNotified(checkpointId, mailboxExecutor::yield);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
            parallelismState.update(
                    Collections.singletonList(getRuntimeContext().getNumberOfParallelSubtasks()));
        }
        statusState.update(Collections.singletonList(status.ordinal()));

        HeadOperatorState currentProcessorState = recordProcessor.snapshotState();
        if (currentProcessorState != null) {
            processorState.update(Collections.singletonList(currentProcessorState));
        } else {
            processorState.clear();
        }

        if (status == HeadOperatorStatus.RUNNING) {
            checkpoints.startLogging(
                    context.getCheckpointId(), context.getRawOperatorStateOutput());
        }

        checkpointAligner
                .onStateSnapshot(context.getCheckpointId())
                .forEach(this::processGloballyAlignedEvent);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        checkpointAligner.abortCheckpoint(checkpointId).forEach(this::processGloballyAlignedEvent);
    }

    @Override
    public void processElement(StreamRecord<IterationRecord<?>> element) throws Exception {
        recordProcessor.processElement(element);
    }

    @Override
    public void processFeedback(StreamRecord<IterationRecord<?>> iterationRecord) throws Exception {
        if (iterationRecord.getValue().getType() == IterationRecord.Type.BARRIER) {
            checkpoints.commitCheckpointsUntil(iterationRecord.getValue().getCheckpointId());
            return;
        }

        checkpoints.append(iterationRecord.getValue());
        boolean terminated = recordProcessor.processFeedbackElement(iterationRecord);
        if (terminated) {
            checkState(status == HeadOperatorStatus.TERMINATING);
            status = HeadOperatorStatus.TERMINATED;
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent operatorEvent) {
        if (operatorEvent instanceof GloballyAlignedEvent) {
            checkpointAligner
                    .checkHoldingGloballyAlignedEvent((GloballyAlignedEvent) operatorEvent)
                    .ifPresent(this::processGloballyAlignedEvent);
        } else if (operatorEvent instanceof CoordinatorCheckpointEvent) {
            checkpointAligner.coordinatorNotify((CoordinatorCheckpointEvent) operatorEvent);
        }
    }

    private void processGloballyAlignedEvent(GloballyAlignedEvent globallyAlignedEvent) {
        boolean shouldTerminate = recordProcessor.onGloballyAligned(globallyAlignedEvent);
        if (shouldTerminate) {
            status = HeadOperatorStatus.TERMINATING;
            recordProcessor = new TerminatingHeadOperatorRecordProcessor();
        }
    }

    @Override
    public void endInput() throws Exception {
        if (status == HeadOperatorStatus.RUNNING) {
            recordProcessor.processElement(
                    new StreamRecord<>(IterationRecord.newEpochWatermark(0, "fake")));
        }

        // Since we choose to block here, we could not continue to process the barriers received
        // from the task inputs, which would block the precedent tasks from finishing since
        // they need to complete their final checkpoint. This is a temporary solution to this issue
        // that we will check the input channels, trigger all the checkpoints until we see
        // the EndOfPartitionEvent.
        checkState(getContainingTask().getEnvironment().getAllInputGates().length == 1);
        checkState(
                getContainingTask()
                                .getEnvironment()
                                .getAllInputGates()[0]
                                .getNumberOfInputChannels()
                        == 1);
        InputChannel inputChannel =
                getContainingTask().getEnvironment().getAllInputGates()[0].getChannel(0);

        boolean endOfPartitionReceived = false;
        long lastTriggerCheckpointId = 0;
        while (!endOfPartitionReceived && status != HeadOperatorStatus.TERMINATED) {
            mailboxExecutor.tryYield();
            Thread.sleep(200);

            List<AbstractEvent> events = parseInputChannelEvents(inputChannel);

            for (AbstractEvent event : events) {
                if (event instanceof CheckpointBarrier) {
                    CheckpointBarrier barrier = (CheckpointBarrier) event;
                    if (barrier.getId() > lastTriggerCheckpointId) {
                        getContainingTask()
                                .triggerCheckpointAsync(
                                        new CheckpointMetaData(
                                                barrier.getId(), barrier.getTimestamp()),
                                        barrier.getCheckpointOptions());
                        lastTriggerCheckpointId = barrier.getId();
                    }

                } else if (event instanceof EndOfPartitionEvent) {
                    endOfPartitionReceived = true;
                }
            }
        }

        // By here we could step into the normal loop.
        while (status != HeadOperatorStatus.TERMINATED) {
            mailboxExecutor.yield();
        }
    }

    @Override
    public void close() throws Exception {
        if (checkpoints != null) {
            checkpoints.close();
        }
    }

    private void registerFeedbackConsumer(Executor mailboxExecutor) {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int attemptNum = getRuntimeContext().getAttemptNumber();
        FeedbackKey<StreamRecord<IterationRecord<?>>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, feedbackIndex);
        SubtaskFeedbackKey<StreamRecord<IterationRecord<?>>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        FeedbackChannel<StreamRecord<IterationRecord<?>>> channel = broker.getChannel(key);
        OperatorUtils.registerFeedbackConsumer(channel, this, mailboxExecutor);
    }

    private List<AbstractEvent> parseInputChannelEvents(InputChannel inputChannel)
            throws Exception {
        List<AbstractEvent> events = new ArrayList<>();
        if (inputChannel instanceof RemoteInputChannel) {
            Class<?> seqBufferClass =
                    Class.forName(
                            "org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel$SequenceBuffer");
            PrioritizedDeque<?> queue =
                    ReflectionUtils.getFieldValue(
                            inputChannel, RemoteInputChannel.class, "receivedBuffers");
            for (Object sequenceBuffer : queue) {
                Buffer buffer =
                        ReflectionUtils.getFieldValue(sequenceBuffer, seqBufferClass, "buffer");
                if (!buffer.isBuffer()) {
                    events.add(EventSerializer.fromBuffer(buffer, getClass().getClassLoader()));
                }
            }
        } else if (inputChannel instanceof LocalInputChannel) {
            PipelinedSubpartitionView subpartitionView =
                    ReflectionUtils.getFieldValue(
                            inputChannel, LocalInputChannel.class, "subpartitionView");
            PipelinedSubpartition pipelinedSubpartition =
                    ReflectionUtils.getFieldValue(
                            subpartitionView, PipelinedSubpartitionView.class, "parent");
            PrioritizedDeque<BufferConsumerWithPartialRecordLength> queue =
                    ReflectionUtils.getFieldValue(
                            pipelinedSubpartition, PipelinedSubpartition.class, "buffers");
            for (BufferConsumerWithPartialRecordLength bufferConsumer : queue) {
                if (!bufferConsumer.getBufferConsumer().isBuffer()) {
                    events.add(
                            EventSerializer.fromBuffer(
                                    bufferConsumer.getBufferConsumer().copy().build(),
                                    getClass().getClassLoader()));
                }
            }
        } else {
            LOG.warn("Unknown input channel type: " + inputChannel);
        }

        return events;
    }

    @VisibleForTesting
    public OperatorEventGateway getOperatorEventGateway() {
        return operatorEventGateway;
    }

    @VisibleForTesting
    MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    @VisibleForTesting
    HeadOperatorRecordProcessor getRecordProcessor() {
        return recordProcessor;
    }

    @VisibleForTesting
    public HeadOperatorStatus getStatus() {
        return status;
    }

    @VisibleForTesting
    enum HeadOperatorStatus {
        RUNNING,

        TERMINATING,

        TERMINATED
    }

    private class ContextImpl implements HeadOperatorRecordProcessor.Context {

        @Override
        public StreamConfig getStreamConfig() {
            return HeadOperator.this.config;
        }

        @Override
        public TaskInfo getTaskInfo() {
            return getContainingTask().getEnvironment().getTaskInfo();
        }

        @Override
        public void output(StreamRecord<IterationRecord<?>> record) {
            output.collect(record);
        }

        @Override
        public void output(
                OutputTag<IterationRecord<?>> outputTag, StreamRecord<IterationRecord<?>> record) {
            output.collect(outputTag, record);
        }

        @Override
        public void broadcastOutput(StreamRecord<IterationRecord<?>> record) {
            try {
                eventBroadcastOutput.broadcastEmit((StreamRecord) record);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Failed to broadcast event", e);
            }
        }

        @Override
        public void updateEpochToCoordinator(int epoch, long numFeedbackRecords) {
            operatorEventGateway.sendEventToCoordinator(
                    new SubtaskAlignedEvent(epoch, numFeedbackRecords, isCriteriaStream));
        }
    }
}
