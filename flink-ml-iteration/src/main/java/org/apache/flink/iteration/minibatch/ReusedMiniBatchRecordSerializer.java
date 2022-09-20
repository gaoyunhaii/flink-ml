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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.typeinfo.ReusedIterationRecordSerializer;

import java.io.IOException;
import java.util.Objects;

/** The serializer of the mini-batch record. */
public class ReusedMiniBatchRecordSerializer<T> extends TypeSerializer<MiniBatchRecord<T>> {

    private final ReusedIterationRecordSerializer<T> iterationRecordSerializer;

    private final MiniBatchRecord<T> reused;

    public ReusedMiniBatchRecordSerializer(
            ReusedIterationRecordSerializer<T> iterationRecordSerializer) {
        this.iterationRecordSerializer = iterationRecordSerializer;
        this.reused = new MiniBatchRecord<>();
    }

    public ReusedIterationRecordSerializer<T> getIterationRecordSerializer() {
        return iterationRecordSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<MiniBatchRecord<T>> duplicate() {
        return new ReusedMiniBatchRecordSerializer<>(iterationRecordSerializer);
    }

    @Override
    public MiniBatchRecord<T> createInstance() {
        return null;
    }

    private void ensureLength(MiniBatchRecord<T> target, int n) {
        if (target.getRecords().size() == n) {
            return;
        }

        if (target.getRecords().size() < n) {
            for (int i = target.getRecords().size(); i < n; ++i) {
                target.getRecords().add(IterationRecord.newRecord(null, 0));
                target.getTimestamps().add(null);
            }

            return;
        }

        target.getRecords().subList(n, target.getRecords().size()).clear();
        target.getTimestamps().subList(n, target.getTimestamps().size()).clear();
    }

    @Override
    public MiniBatchRecord<T> copy(MiniBatchRecord<T> from) {
        MiniBatchRecord<T> copied = new MiniBatchRecord<>();
        for (int i = 0; i < from.getRecords().size(); ++i) {
            copied.getRecords().add(iterationRecordSerializer.copy(from.getRecords().get(i)));
            copied.getTimestamps().add(from.getTimestamps().get(i));
        }

        return copied;
    }

    @Override
    public MiniBatchRecord<T> copy(MiniBatchRecord<T> from, MiniBatchRecord<T> reused) {
        ensureLength(reused, from.getRecords().size());
        for (int i = 0; i < from.getRecords().size(); ++i) {
            iterationRecordSerializer.copy(from.getRecords().get(i), reused.getRecords().get(i));
            reused.getTimestamps().set(i, from.getTimestamps().get(i));
        }

        return reused;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(MiniBatchRecord<T> miniBatchRecord, DataOutputView dataOutputView)
            throws IOException {
        dataOutputView.writeInt(miniBatchRecord.getSize());
        //        for (int i = 0; i < miniBatchRecord.getSize(); ++i) {
        //            if (miniBatchRecord.getTimestamps().get(i) != null) {
        //                dataOutputView.writeByte(0);
        //                dataOutputView.writeLong(miniBatchRecord.getTimestamps().get(i));
        //            } else {
        //                dataOutputView.writeByte(1);
        //            }
        //        }

        for (int i = 0; i < miniBatchRecord.getSize(); ++i) {
            iterationRecordSerializer.serialize(
                    miniBatchRecord.getRecords().get(i), dataOutputView);
        }
    }

    @Override
    public MiniBatchRecord<T> deserialize(DataInputView dataInputView) throws IOException {
        return deserialize(this.reused, dataInputView);
    }

    @Override
    public MiniBatchRecord<T> deserialize(MiniBatchRecord<T> tmpReused, DataInputView dataInputView)
            throws IOException {
        int size = dataInputView.readInt();
        ensureLength(tmpReused, size);

        //        for (int i = 0; i < size; ++i) {
        //            byte hasTimestampTag = dataInputView.readByte();
        //            if (hasTimestampTag == 0) {
        //                tmpReused.getTimestamps().set(i, dataInputView.readLong());
        //            } else {
        //                tmpReused.getTimestamps().set(i, null);
        //            }
        //        }

        for (int i = 0; i < size; ++i) {
            iterationRecordSerializer.deserialize(tmpReused.getRecords().get(i), dataInputView);
        }

        return tmpReused;
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView)
            throws IOException {
        MiniBatchRecord<T> record = deserialize(dataInputView);
        serialize(record, dataOutputView);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReusedMiniBatchRecordSerializer<?> that = (ReusedMiniBatchRecordSerializer<?>) o;
        return Objects.equals(iterationRecordSerializer, that.iterationRecordSerializer);
    }

    @Override
    public int hashCode() {
        return iterationRecordSerializer != null ? iterationRecordSerializer.hashCode() : 0;
    }

    @Override
    public TypeSerializerSnapshot<MiniBatchRecord<T>> snapshotConfiguration() {
        return new ReusedMiniBatchRecordTypeSerializerSnapshot<>();
    }

    private static final class ReusedMiniBatchRecordTypeSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<
                    MiniBatchRecord<T>, ReusedMiniBatchRecordSerializer<T>> {

        private static final int CURRENT_VERSION = 1;

        public ReusedMiniBatchRecordTypeSerializerSnapshot() {
            super(ReusedMiniBatchRecordSerializer.class);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return CURRENT_VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                ReusedMiniBatchRecordSerializer<T> miniBatchRecordSerializer) {
            return new TypeSerializer[] {miniBatchRecordSerializer.iterationRecordSerializer};
        }

        @Override
        protected ReusedMiniBatchRecordSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] typeSerializers) {
            ReusedIterationRecordSerializer<T> elementSerializer =
                    (ReusedIterationRecordSerializer<T>) typeSerializers[0];
            return new ReusedMiniBatchRecordSerializer<>(elementSerializer);
        }
    }
}
