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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Output records for the mini-batch style iteration. */
public class MiniBatchOutputOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<MiniBatchRecord<T>, T> {

    private transient StreamRecord<T> reusable;

    public MiniBatchOutputOperator() {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.reusable = new StreamRecord<>(null);
    }

    @Override
    public void processElement(StreamRecord<MiniBatchRecord<T>> streamRecord) throws Exception {
        boolean needDealt =
                streamRecord.getValue().getRecords().size() > 0
                        && streamRecord.getValue().getRecords().get(0).getType()
                                == IterationRecord.Type.RECORD;
        if (needDealt) {
            for (int i = 0; i < streamRecord.getValue().getRecords().size(); ++i) {
                Long timestamp = streamRecord.getValue().getTimestamps().get(i);
                if (timestamp != null) {
                    reusable.setTimestamp(timestamp);
                }

                reusable.replace(streamRecord.getValue().getRecords().get(i).getValue());
                output.collect(reusable);
            }
        }
    }
}
