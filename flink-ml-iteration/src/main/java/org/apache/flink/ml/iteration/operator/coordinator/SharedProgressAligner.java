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

package org.apache.flink.ml.iteration.operator.coordinator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.ml.iteration.IterationID;
import org.apache.flink.ml.iteration.operator.event.CoordinatorCheckpointEvent;
import org.apache.flink.ml.iteration.operator.event.GloballyAlignedEvent;
import org.apache.flink.ml.iteration.operator.event.SubtaskAlignedEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The progress aligner shared between multiple {@link HeadOperatorCoordinator}. It maintains the
 * information for each round, once one round is aligned, it would notify all the register
 * listenerss.
 */
public class SharedProgressAligner {

    private static final Logger LOG = LoggerFactory.getLogger(SharedProgressAligner.class);

    public static ConcurrentHashMap<IterationID, SharedProgressAligner> instances =
            new ConcurrentHashMap<>();

    private final IterationID iterationId;

    private final int totalHeadParallelism;

    private final OperatorCoordinator.Context context;

    private final Executor executor;

    private final Map<Integer, RoundStatus> statusByRound;

    private final Map<OperatorID, SharedProgressAlignerListener> listeners;

    private final Map<Long, CheckpointStatus> checkpointStatuses;

    public static SharedProgressAligner getOrCreate(
            IterationID iterationId,
            int totalHeadParallelism,
            OperatorCoordinator.Context context,
            Supplier<Executor> executorFactory) {
        return instances.computeIfAbsent(
                iterationId,
                ignored ->
                        new SharedProgressAligner(
                                iterationId, totalHeadParallelism, context, executorFactory.get()));
    }

    @VisibleForTesting
    static ConcurrentHashMap<IterationID, SharedProgressAligner> getInstances() {
        return instances;
    }

    private SharedProgressAligner(
            IterationID iterationId,
            int totalHeadParallelism,
            OperatorCoordinator.Context context,
            Executor executor) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.totalHeadParallelism = totalHeadParallelism;
        this.context = Objects.requireNonNull(context);
        this.executor = Objects.requireNonNull(executor);

        this.statusByRound = new HashMap<>();
        this.listeners = new HashMap<>();
        this.checkpointStatuses = new HashMap<>();
    }

    public void registerAlignedConsumer(
            OperatorID operatorID, SharedProgressAlignerListener alignedConsumer) {
        runInEventLoop(
                () -> this.listeners.put(operatorID, alignedConsumer),
                "Register listeners %s",
                operatorID.toHexString());
    }

    public void unregisterConsumer(OperatorID operatorID) {
        synchronized (this) {
            runInEventLoop(
                    () -> {
                        this.listeners.remove(operatorID);
                        if (listeners.isEmpty()) {
                            instances.remove(iterationId);
                        }
                    },
                    "Unregister listeners %s",
                    operatorID.toHexString());
        }
    }

    public void reportSubtaskProgress(
            OperatorID operatorId, int subtaskIndex, SubtaskAlignedEvent subtaskAlignedEvent) {
        runInEventLoop(
                () -> {
                    LOG.debug(
                            "Processing {} from {}-{}",
                            subtaskAlignedEvent,
                            operatorId,
                            subtaskIndex);
                    RoundStatus roundStatus =
                            statusByRound.computeIfAbsent(
                                    subtaskAlignedEvent.getRound(),
                                    round -> new RoundStatus(round, totalHeadParallelism));
                    boolean globallyAligned =
                            roundStatus.report(operatorId, subtaskIndex, subtaskAlignedEvent);
                    if (globallyAligned) {
                        GloballyAlignedEvent globallyAlignedEvent =
                                new GloballyAlignedEvent(
                                        subtaskAlignedEvent.getRound(), roundStatus.isTerminated());
                        for (SharedProgressAlignerListener listeners : listeners.values()) {
                            listeners.onAligned(globallyAlignedEvent);
                        }
                    }
                },
                "Report subtask %s-%d",
                operatorId.toHexString(),
                subtaskIndex);
    }

    public void requestCheckpoint(
            long checkpointId,
            int operatorParallelism,
            CompletableFuture<byte[]> snapshotStateFuture) {
        runInEventLoop(
                () -> {
                    LOG.info("request {} for parallelism {}", checkpointId, operatorParallelism);
                    CheckpointStatus checkpointStatus =
                            checkpointStatuses.computeIfAbsent(
                                    checkpointId,
                                    ignored -> new CheckpointStatus(totalHeadParallelism));
                    boolean aligned =
                            checkpointStatus.notify(operatorParallelism, snapshotStateFuture);
                    if (aligned) {
                        CoordinatorCheckpointEvent checkpointEvent =
                                new CoordinatorCheckpointEvent(checkpointId);
                        for (SharedProgressAlignerListener listener : listeners.values()) {
                            listener.onCheckpointAligned(checkpointEvent);
                        }

                        for (CompletableFuture<byte[]> stateFuture :
                                checkpointStatus.getStateFutures()) {
                            stateFuture.complete(new byte[0]);
                        }

                        checkpointStatuses.remove(checkpointId);
                    }
                },
                "Coordinator report checkpoint %d",
                checkpointId);
    }

    private void runInEventLoop(
            ThrowingRunnable<Throwable> action,
            String actionName,
            Object... actionNameFormatParameters) {
        executor.execute(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                        String actionString = String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "Uncaught exception in the SharedProgressAligner for iteration {} while {}. Triggering job failover.",
                                iterationId,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    @VisibleForTesting
    int getNumberListeners() {
        return listeners.size();
    }

    private static class RoundStatus {

        private final int round;

        private final long totalHeadParallelism;

        private final Map<OperatorInstanceID, SubtaskAlignedEvent> reportedSubtasks;

        public RoundStatus(int round, long totalHeadParallelism) {
            this.round = round;
            this.totalHeadParallelism = totalHeadParallelism;
            this.reportedSubtasks = new HashMap<>();
        }

        public boolean report(OperatorID operatorID, int subtaskIndex, SubtaskAlignedEvent event) {
            reportedSubtasks.put(new OperatorInstanceID(subtaskIndex, operatorID), event);
            checkState(
                    reportedSubtasks.size() <= totalHeadParallelism,
                    "Received more subtasks"
                            + reportedSubtasks
                            + "than the expected total parallelism "
                            + totalHeadParallelism);
            return reportedSubtasks.size() == totalHeadParallelism;
        }

        public boolean isTerminated() {
            checkState(
                    reportedSubtasks.size() == totalHeadParallelism,
                    "The round is not globally aligned yet");

            // We never terminate at round 0.
            if (round == 0) {
                return false;
            }

            long totalRecord = 0;
            boolean hasCriteriaStream = false;
            long totalCriteriaRecord = 0;

            for (SubtaskAlignedEvent event : reportedSubtasks.values()) {
                totalRecord += event.getNumRecordsThisRound();
                if (event.isCriteriaStream()) {
                    hasCriteriaStream = true;
                    totalCriteriaRecord += event.getNumRecordsThisRound();
                }
            }

            return totalRecord == 0 || (hasCriteriaStream && totalCriteriaRecord == 0);
        }
    }

    private static class CheckpointStatus {

        private final long totalHeadParallelism;

        private final List<CompletableFuture<byte[]>> stateFutures = new ArrayList<>();

        private int notifiedCoordinatorParallelism;

        private CheckpointStatus(long totalHeadParallelism) {
            this.totalHeadParallelism = totalHeadParallelism;
        }

        public boolean notify(int parallelism, CompletableFuture<byte[]> stateFuture) {
            stateFutures.add(stateFuture);
            notifiedCoordinatorParallelism += parallelism;

            return notifiedCoordinatorParallelism == totalHeadParallelism;
        }

        public List<CompletableFuture<byte[]>> getStateFutures() {
            return stateFutures;
        }
    }
}
