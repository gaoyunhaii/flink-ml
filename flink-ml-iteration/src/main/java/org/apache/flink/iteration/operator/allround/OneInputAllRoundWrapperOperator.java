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

package org.apache.flink.iteration.operator.allround;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.minibatch.operator.adapter.OneInputMiniBatchProcessor;
import org.apache.flink.iteration.proxy.IterationWrapperOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.List;

/** All-round wrapper for the one-input operator. */
public class OneInputAllRoundWrapperOperator<IN, OUT>
        extends AbstractAllRoundWrapperOperator<OUT, OneInputStreamOperator<IN, OUT>>
        implements OneInputStreamOperator<IterationRecord<IN>, IterationRecord<OUT>>,
                BoundedOneInput,
                OneInputMiniBatchProcessor<IN>,
                IterationWrapperOperator<IN> {

    private final StreamRecord<IN> reusedInput;
    private transient KeySelector<?, ?> stateKeySelector;

    public OneInputAllRoundWrapperOperator(
            StreamOperatorParameters<IterationRecord<OUT>> parameters,
            StreamOperatorFactory<OUT> operatorFactory) {
        super(parameters, operatorFactory);
        this.reusedInput = new StreamRecord<>(null, 0);

        this.stateKeySelector =
                parameters
                        .getStreamConfig()
                        .getStatePartitioner(
                                0, parameters.getContainingTask().getUserCodeClassLoader());
    }

    @Override
    public void processElement(StreamRecord<IterationRecord<IN>> element) throws Exception {
        switch (element.getValue().getType()) {
            case RECORD:
                reusedInput.replace(element.getValue().getValue(), element.getTimestamp());
                setIterationContextRound(element.getValue().getEpoch());
                wrappedOperator.processElement(reusedInput);
                clearIterationContextRound();
                break;
            case EPOCH_WATERMARK:
                onEpochWatermarkEvent(0, element.getValue());
                break;
            default:
                throw new FlinkRuntimeException("Not supported iteration record type: " + element);
        }
    }

    @Override
    public void processRecord(int epoch, List<IterationRecord<IN>> iterationRecords)
            throws Exception {
        setIterationContextRound(epoch);
        if (stateKeySelector != null) {
            for (IterationRecord<IN> record : iterationRecords) {
                reusedInput.replace(record.getValue());
                wrappedOperator.setKeyContextElement(reusedInput);
                wrappedOperator.processElement(reusedInput);
            }
        } else {
            for (IterationRecord<IN> record : iterationRecords) {
                reusedInput.replace(record.getValue());
                wrappedOperator.processElement(reusedInput);
            }
        }
        clearIterationContextRound();
    }

    @Override
    public Input<IN> getWrappedOperator() {
        return wrappedOperator;
    }

    @Override
    public void processEpochWatermark(IterationRecord<IN> epochWatermark) throws IOException {
        onEpochWatermarkEvent(0, epochWatermark);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        wrappedOperator.processWatermark(mark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        wrappedOperator.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker(latencyMarker);
    }

    @Override
    public void setKeyContextElement(StreamRecord<IterationRecord<IN>> record) throws Exception {
        if (record.getValue().getType() == IterationRecord.Type.RECORD) {
            reusedInput.replace(record.getValue().getValue(), record.getTimestamp());
            wrappedOperator.setKeyContextElement(reusedInput);
        }
    }

    @Override
    public void endInput() throws Exception {
        if (wrappedOperator instanceof BoundedOneInput) {
            ((BoundedOneInput) wrappedOperator).endInput();
        }
    }
}
