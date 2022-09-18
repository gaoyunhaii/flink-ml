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
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.ArrayList;
import java.util.List;

public class MultiInputMiniBatchWrapperOperator<OUT>
        extends AbstractMiniBatchWrapperOperator<
                OUT, MultipleInputStreamOperator<IterationRecord<OUT>>>
        implements MultipleInputStreamOperator<MiniBatchRecord<OUT>> {

    public MultiInputMiniBatchWrapperOperator(
            StreamOperatorParameters<MiniBatchRecord<OUT>> parameters,
            StreamOperatorFactory<IterationRecord<OUT>> operatorFactory,
            int miniBatchRecords) {
        super(parameters, operatorFactory, miniBatchRecords);
    }

    private <IN> void processElement(
            int inputIndex,
            Input<IterationRecord<IN>> input,
            StreamRecord<IterationRecord<IN>> reusedInput,
            StreamRecord<MiniBatchRecord<IN>> element)
            throws Exception {
        for (IterationRecord<IN> record : element.getValue().getRecords()) {
            reusedInput.replace(record);
            input.setKeyContextElement(reusedInput);
            input.processElement(reusedInput);
        }
    }

    @Override
    public List<Input> getInputs() {
        List<Input> proxyInputs = new ArrayList<>();
        for (int i = 0; i < wrappedOperator.getInputs().size(); ++i) {
            // TODO: Note that here we relies on the assumption that the
            // stream graph generator labels the input from 1 to n for
            // the input array, which we map them from 0 to n - 1.
            proxyInputs.add(new ProxyInput(i));
        }
        return proxyInputs;
    }

    @Override
    public void endInput(int i) throws Exception {
        super.endInput(i);

        if (wrappedOperator instanceof BoundedMultiInput) {
            ((BoundedMultiInput) wrappedOperator).endInput(i);
        }
    }

    private class ProxyInput<IN> implements Input<MiniBatchRecord<IN>> {

        private final int inputIndex;

        private final StreamRecord<IterationRecord<IN>> reusedInput;

        private final Input<IterationRecord<IN>> input;

        public ProxyInput(int inputIndex) {
            this.inputIndex = inputIndex;
            this.reusedInput = new StreamRecord<>(null, 0);
            this.input = wrappedOperator.getInputs().get(inputIndex);
        }

        @Override
        public void processElement(StreamRecord<MiniBatchRecord<IN>> element) throws Exception {
            MultiInputMiniBatchWrapperOperator.this.processElement(
                    inputIndex, input, reusedInput, element);
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            input.processWatermark(mark);
        }

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            input.processWatermarkStatus(watermarkStatus);
        }

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            input.processLatencyMarker(latencyMarker);
        }

        @Override
        public void setKeyContextElement(StreamRecord<MiniBatchRecord<IN>> record)
                throws Exception {
            // Do nothing.
        }
    }
}
