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

package org.apache.flink.iteration.minibatch.proxy;

import org.apache.flink.iteration.minibatch.MiniBatchRecord;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MiniBatchCalculatedStreamPartitioner<T> extends StreamPartitioner<MiniBatchRecord<T>> {

    private final StreamPartitioner<T> wrappedStreamPartitioner;

    private int numberOfChannels;

    public MiniBatchCalculatedStreamPartitioner(StreamPartitioner<T> wrappedStreamPartitioner) {
        this.wrappedStreamPartitioner = wrappedStreamPartitioner;
    }

    public StreamPartitioner<T> getWrappedStreamPartitioner() {
        return wrappedStreamPartitioner;
    }

    public int getNumberOfChannels() {
        return numberOfChannels;
    }

    @Override
    public StreamPartitioner<MiniBatchRecord<T>> copy() {
        return new MiniBatchCalculatedStreamPartitioner<>(wrappedStreamPartitioner.copy());
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return wrappedStreamPartitioner.getDownstreamSubtaskStateMapper();
    }

    @Override
    public boolean isPointwise() {
        return wrappedStreamPartitioner.isPointwise();
    }

    @Override
    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);
        wrappedStreamPartitioner.setup(numberOfChannels);
        this.numberOfChannels = numberOfChannels;
    }

    @Override
    public int selectChannel(
            SerializationDelegate<StreamRecord<MiniBatchRecord<T>>>
                    streamRecordSerializationDelegate) {
        // Notes we relies on the sender to set it!!
        return streamRecordSerializationDelegate.getInstance().getValue().getTargetPartition();
    }
}
