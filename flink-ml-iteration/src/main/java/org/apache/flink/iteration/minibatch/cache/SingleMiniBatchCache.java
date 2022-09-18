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

package org.apache.flink.iteration.minibatch.cache;

import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class SingleMiniBatchCache implements MiniBatchCache {

    private final Output<StreamRecord<MiniBatchRecord<?>>> innerOutput;

    private final int miniBatchRecords;

    private final StreamRecord<MiniBatchRecord<?>> reused;

    public SingleMiniBatchCache(
            Output<StreamRecord<MiniBatchRecord<?>>> innerOutput,
            int miniBatchRecords,
            int targetPartition) {
        this.innerOutput = innerOutput;
        this.miniBatchRecords = miniBatchRecords;
        reused = new StreamRecord<>(new MiniBatchRecord<>());
        reused.getValue().setTargetPartition(targetPartition);
    }

    @Override
    public void collect(IterationRecord<?> iterationRecord, Long timestamp) {
        reused.getValue().addRecord((IterationRecord) iterationRecord, timestamp);
        if (reused.getValue().getSize() >= miniBatchRecords) {
            innerOutput.collect(reused);
            reused.getValue().clear();
        }
    }

    @Override
    public void flush() {
        innerOutput.collect(reused);
        reused.getValue().clear();
    }
}
