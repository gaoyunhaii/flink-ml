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
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

public class MultiMiniBatchCache implements MiniBatchCache {

    private final StreamPartitioner<?> partitioner;

    private final List<SingleMiniBatchCache> caches;

    private final SerializationDelegate<StreamRecord<?>> serializationDelegate;

    public MultiMiniBatchCache(
            Output<StreamRecord<MiniBatchRecord<?>>> innerOutput,
            StreamPartitioner<?> partitioner,
            int numberOfChannels,
            int miniBatchRecords) {
        this.partitioner = partitioner;

        this.caches = new ArrayList<>();
        for (int i = 0; i < numberOfChannels; ++i) {
            caches.add(new SingleMiniBatchCache(innerOutput, miniBatchRecords, i));
        }

        serializationDelegate = new SerializationDelegate<>(null);
    }

    @Override
    public void collect(IterationRecord<?> iterationRecord, Long timestamp) {
        // Let's check its target partition
        serializationDelegate.getInstance().replace(iterationRecord.getValue());
        int targetPartition =
                partitioner.selectChannel((SerializationDelegate) serializationDelegate);
        caches.get(targetPartition).collect(iterationRecord, timestamp);
    }

    @Override
    public void flush() {
        // now let's do it
        for (SingleMiniBatchCache cache : caches) {
            cache.flush();
        }
    }
}
