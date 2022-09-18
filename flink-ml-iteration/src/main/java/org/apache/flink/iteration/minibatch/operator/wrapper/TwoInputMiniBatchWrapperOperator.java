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
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

public class TwoInputMiniBatchWrapperOperator<IN1, IN2, OUT>
        extends AbstractMiniBatchWrapperOperator<
                OUT,
                TwoInputStreamOperator<
                        IterationRecord<IN1>, IterationRecord<IN2>, IterationRecord<OUT>>>
        implements TwoInputStreamOperator<
                MiniBatchRecord<IN1>, MiniBatchRecord<IN2>, MiniBatchRecord<OUT>> {

    private final StreamRecord<IterationRecord<IN1>> reusedInput1;

    private final StreamRecord<IterationRecord<IN2>> reusedInput2;

    public TwoInputMiniBatchWrapperOperator(
            StreamOperatorParameters<MiniBatchRecord<OUT>> parameters,
            StreamOperatorFactory<IterationRecord<OUT>> operatorFactory,
            int miniBatchRecords) {
        super(parameters, operatorFactory, miniBatchRecords);
        this.reusedInput1 = new StreamRecord<>(null, 0);
        this.reusedInput2 = new StreamRecord<>(null, 0);
    }

    @Override
    public void processElement1(StreamRecord<MiniBatchRecord<IN1>> streamRecord) throws Exception {
        for (IterationRecord<IN1> record : streamRecord.getValue().getRecords()) {
            reusedInput1.replace(record);
            wrappedOperator.setKeyContextElement1(reusedInput1);
            wrappedOperator.processElement1(reusedInput1);
        }
    }

    @Override
    public void processElement2(StreamRecord<MiniBatchRecord<IN2>> streamRecord) throws Exception {
        for (IterationRecord<IN2> record : streamRecord.getValue().getRecords()) {
            reusedInput2.replace(record);
            wrappedOperator.setKeyContextElement2(reusedInput2);
            wrappedOperator.processElement2(reusedInput2);
        }
    }

    @Override
    public void processWatermark1(Watermark watermark) throws Exception {
        wrappedOperator.processWatermark1(watermark);
    }

    @Override
    public void processWatermark2(Watermark watermark) throws Exception {
        wrappedOperator.processWatermark2(watermark);
    }

    @Override
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker1(latencyMarker);
    }

    @Override
    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker2(latencyMarker);
    }

    @Override
    public void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        wrappedOperator.processWatermarkStatus1(watermarkStatus);
    }

    @Override
    public void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        wrappedOperator.processWatermarkStatus2(watermarkStatus);
    }
}
