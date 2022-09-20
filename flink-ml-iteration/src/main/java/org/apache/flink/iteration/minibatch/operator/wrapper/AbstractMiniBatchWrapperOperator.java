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

package org.apache.flink.iteration.minibatch.operator.wrapper;

import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.iteration.minibatch.cache.MiniBatchOutputFactory;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/** Base class for mini-batch wrapper */
public abstract class AbstractMiniBatchWrapperOperator<
                T, S extends StreamOperator<IterationRecord<T>>>
        implements StreamOperator<MiniBatchRecord<T>> {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractMiniBatchWrapperOperator.class);

    protected final StreamOperatorParameters<MiniBatchRecord<T>> parameters;

    protected final StreamConfig streamConfig;

    protected final StreamTask<?, ?> containingTask;

    protected final StreamOperatorFactory<IterationRecord<T>> operatorFactory;

    protected final Output<StreamRecord<MiniBatchRecord<T>>> providedOutput;

    protected final Output<StreamRecord<IterationRecord<T>>> miniBatchedOutput;

    protected final S wrappedOperator;

    /** Metric group for the operator. */
    protected final InternalOperatorMetricGroup metrics;

    public AbstractMiniBatchWrapperOperator(
            StreamOperatorParameters<MiniBatchRecord<T>> parameters,
            StreamOperatorFactory<IterationRecord<T>> operatorFactory,
            int miniBatchRecords) {
        this.parameters = Objects.requireNonNull(parameters);
        this.streamConfig = Objects.requireNonNull(parameters.getStreamConfig());
        this.containingTask = Objects.requireNonNull(parameters.getContainingTask());
        this.providedOutput = Objects.requireNonNull(parameters.getOutput());
        this.operatorFactory = Objects.requireNonNull(operatorFactory);

        this.metrics = createOperatorMetricGroup(containingTask.getEnvironment(), streamConfig);

        this.miniBatchedOutput =
                MiniBatchOutputFactory.createIterationRecordOutput(
                        (Output) providedOutput,
                        miniBatchRecords,
                        metrics.getIOMetricGroup().getNumRecordsOutCounter(),
                        streamConfig);
        this.wrappedOperator =
                (S)
                        StreamOperatorFactoryUtil.<IterationRecord<T>, S>createOperator(
                                        operatorFactory,
                                        (StreamTask) parameters.getContainingTask(),
                                        OperatorUtils.createWrappedMiniBatchOperatorConfig(
                                                parameters.getStreamConfig()),
                                        miniBatchedOutput,
                                        parameters.getOperatorEventDispatcher())
                                .f0;
    }

    private InternalOperatorMetricGroup createOperatorMetricGroup(
            Environment environment, StreamConfig streamConfig) {
        try {
            InternalOperatorMetricGroup operatorMetricGroup =
                    environment
                            .getMetricGroup()
                            .getOrAddOperator(
                                    streamConfig.getOperatorID(), streamConfig.getOperatorName());
            if (streamConfig.isChainEnd()) {
                operatorMetricGroup.getIOMetricGroup().reuseOutputMetricsForTask();
            }
            return operatorMetricGroup;
        } catch (Exception e) {
            LOG.warn("An error occurred while instantiating task metrics.", e);
            return UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        }
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateInitializer)
            throws Exception {
        wrappedOperator.initializeState(streamTaskStateInitializer);
    }

    @Override
    public void open() throws Exception {
        wrappedOperator.open();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        wrappedOperator.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        return wrappedOperator.snapshotState(
                checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void finish() throws Exception {
        wrappedOperator.finish();
    }

    @Override
    public void close() throws Exception {
        wrappedOperator.close();
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return metrics;
    }

    @Override
    public OperatorID getOperatorID() {
        return wrappedOperator.getOperatorID();
    }

    @Override
    public void setKeyContextElement2(StreamRecord streamRecord) throws Exception {
        // Do nothing.
    }

    @Override
    public void setKeyContextElement1(StreamRecord streamRecord) throws Exception {
        // Do nothing.
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        wrappedOperator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void setCurrentKey(Object o) {
        wrappedOperator.setCurrentKey(o);
    }

    @Override
    public Object getCurrentKey() {
        return wrappedOperator.getCurrentKey();
    }
}
