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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

public class SingleMiniBatchCache implements MiniBatchCache {

    private final Collector<StreamRecord<MiniBatchRecord<?>>> innerOutput;

    private final int miniBatchRecords;

    private final StreamRecord<MiniBatchRecord<?>> reused;

    private final List<IterationRecord<?>> reusedIterationRecords;

    private int nextIterationRecordToUse;

    public SingleMiniBatchCache(
            Collector<StreamRecord<MiniBatchRecord<?>>> innerOutput,
            OutputTag<?> tag,
            int miniBatchRecords,
            int targetPartition,
            TypeSerializer<?> typeSerializer) {
        this.innerOutput = innerOutput;
        this.miniBatchRecords = miniBatchRecords;

        reused = new StreamRecord<>(new MiniBatchRecord<>());
        reused.getValue().setTargetPartition(targetPartition);

        reusedIterationRecords = new ArrayList<>(miniBatchRecords);
        for (int i = 0; i < miniBatchRecords; ++i) {
            reusedIterationRecords.add(IterationRecord.newRecord(null, 0));
        }
        nextIterationRecordToUse = 0;
    }

    @Override
    public void collect(IterationRecord<?> iterationRecord, Long timestamp) {
        checkState(nextIterationRecordToUse < reusedIterationRecords.size());
        IterationRecord reusedIterationRecord =
                reusedIterationRecords.get(nextIterationRecordToUse++);

        // Let's deal with the serializer
        reusedIterationRecord.setType(iterationRecord.getType());
        reusedIterationRecord.setEpoch(iterationRecord.getEpoch());
        reusedIterationRecord.setValue(iterationRecord.getValue());
        reusedIterationRecord.setSender(iterationRecord.getSender());
        reusedIterationRecord.setCheckpointId(iterationRecord.getCheckpointId());

        reused.getValue().addRecord(reusedIterationRecord, timestamp);
        if (reused.getValue().getSize() >= miniBatchRecords) {
            innerOutput.collect(reused);
            reused.getValue().clear();
            nextIterationRecordToUse = 0;
        }
    }

    @Override
    public void flush() {
        if (reused.getValue().getSize() > 0) {
            innerOutput.collect(reused);
            reused.getValue().clear();
            nextIterationRecordToUse = 0;
        }
    }
}
