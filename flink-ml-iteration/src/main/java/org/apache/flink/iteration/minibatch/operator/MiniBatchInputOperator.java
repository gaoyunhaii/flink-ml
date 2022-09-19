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

import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.iteration.minibatch.cache.SingleMiniBatchCache;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MiniBatchInputOperator<T> extends AbstractStreamOperator<MiniBatchRecord<T>>
        implements OneInputStreamOperator<T, MiniBatchRecord<T>> {

    private final int miniBatchRecords;

    private transient IterationRecord<T> reused;

    private transient SingleMiniBatchCache miniBatchCache;

    public MiniBatchInputOperator(int miniBatchRecords) {
        this.miniBatchRecords = miniBatchRecords;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.reused = IterationRecord.newRecord(null, 0);
        this.miniBatchCache = new SingleMiniBatchCache((Output) output, null, miniBatchRecords, -1);
    }

    @Override
    public void processElement(StreamRecord<T> streamRecord) throws Exception {
        //        System.out.println(
        //                Thread.currentThread()
        //                        + "input "
        //                        + streamRecord.getValue()
        //                        + ", "
        //                        + miniBatchRecords);
        reused.setValue(streamRecord.getValue());
        miniBatchCache.collect(
                reused, streamRecord.hasTimestamp() ? streamRecord.getTimestamp() : null);
    }

    @Override
    public void finish() throws Exception {
        miniBatchCache.flush();
    }
}
