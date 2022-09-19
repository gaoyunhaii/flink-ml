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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

public class OneInputMiniBatchWrapperOperator<IN, OUT>
        extends AbstractMiniBatchWrapperOperator<
                OUT, OneInputStreamOperator<IterationRecord<IN>, IterationRecord<OUT>>>
        implements OneInputStreamOperator<MiniBatchRecord<IN>, MiniBatchRecord<OUT>> {

    private final StreamRecord<IterationRecord<IN>> reused;

    public OneInputMiniBatchWrapperOperator(
            StreamOperatorParameters<MiniBatchRecord<OUT>> parameters,
            StreamOperatorFactory<IterationRecord<OUT>> operatorFactory,
            int miniBatchRecords) {
        super(parameters, operatorFactory, miniBatchRecords);
        reused = new StreamRecord<>(null, 0);
    }

    @Override
    public void processElement(StreamRecord<MiniBatchRecord<IN>> streamRecord) throws Exception {
        // System.out.println(getOperatorID() + " is processing " + streamRecord);
        for (IterationRecord<IN> record : streamRecord.getValue().getRecords()) {
            reused.replace(record);
            wrappedOperator.setKeyContextElement(reused);
            wrappedOperator.processElement(reused);
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        wrappedOperator.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        wrappedOperator.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker(latencyMarker);
    }
}
