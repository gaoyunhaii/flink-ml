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

package org.apache.flink.iteration.minibatch;

import org.apache.flink.iteration.IterationRecord;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Collectors;

/** Which contains a list of iteration records. We force it to contains only one type of records. */
public class MiniBatchRecord<T> implements Serializable {

    private transient int targetPartition;

    private final ArrayList<IterationRecord<T>> records;

    private final ArrayList<Long> timestamps;

    public MiniBatchRecord() {
        this.records = new ArrayList<>();
        this.timestamps = new ArrayList<>();
    }

    public int getTargetPartition() {
        return targetPartition;
    }

    public void setTargetPartition(int targetPartition) {
        this.targetPartition = targetPartition;
    }

    public ArrayList<IterationRecord<T>> getRecords() {
        return records;
    }

    public ArrayList<Long> getTimestamps() {
        return timestamps;
    }

    public void addRecord(IterationRecord<T> record, Long timestamp) {
        records.add(record);
        timestamps.add(timestamp);
    }

    public void clear() {
        records.clear();
        timestamps.clear();
    }

    public int getSize() {
        return records.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MiniBatchRecord)) {
            return false;
        }

        MiniBatchRecord<?> that = (MiniBatchRecord<?>) o;
        return targetPartition == that.targetPartition
                && Objects.equals(records, that.records)
                && Objects.equals(timestamps, that.timestamps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetPartition, records, timestamps);
    }

    @Override
    public String toString() {
        return "MiniBatchRecord{"
                + "targetPartition="
                + targetPartition
                + ", records=[\n"
                + StringUtils.join(
                        records.stream().map(r -> "\t" + r.toString()).collect(Collectors.toList()),
                        "\n")
                + "], timestamps ="
                + timestamps
                + '}';
    }
}
