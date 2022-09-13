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

package org.apache.flink.iteration.typeinfo;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.iteration.IterationRecord;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SPTest {

    private static List<byte[]> ARRAYS = new ArrayList<>();

    private static final int COUNT = 1000_0000;

    private static final int ARRAY_SIZE = 128;

    static {
        for (int i = 0; i < 26; ++i) {
            byte[] record = new byte[ARRAY_SIZE];
            for (int j = 0; j < record.length; ++j) {
                record[j] = (byte) ('a' + (i + j) % 26);
            }
            ARRAYS.add(record);
        }
    }

    @Test
    public void rawTest() throws IOException {
        BytePrimitiveArraySerializer rawSerializer = BytePrimitiveArraySerializer.INSTANCE;
        DataOutputSerializer ds = new DataOutputSerializer(128);
        DataInputDeserializer din = new DataInputDeserializer();
        long now = -System.nanoTime();

        int totalLength = 0;
        for (int i = 0; i < 1_0000_0000; ++i) {
            ds.setPositionUnsafe(0);
            rawSerializer.serialize(ARRAYS.get(i % 26), ds);

            din.setBuffer(ds.getSharedBuffer());
            byte[] array = rawSerializer.deserialize(din);
            totalLength += array.length;
        }
        now += System.nanoTime();
        System.out.println("time used " + now / 1e6 + "ms");
        System.out.println("total length " + totalLength);
    }

    @Test
    public void wrapTest() throws IOException {
        BytePrimitiveArraySerializer rawSerializer = BytePrimitiveArraySerializer.INSTANCE;
        DataOutputSerializer ds = new DataOutputSerializer(128);
        DataInputDeserializer din = new DataInputDeserializer();

        IterationRecord<byte[]> reused = IterationRecord.newRecord(null, 0);
        ReusedIterationRecordSerializer<byte[]> wrapSerializer =
                new ReusedIterationRecordSerializer<>(rawSerializer);

        long totalLength = 0;

        long now = -System.nanoTime();
        for (int i = 0; i < 1_0000_0000; ++i) {
            ds.setPositionUnsafe(0);
            reused.setValue(ARRAYS.get(i % 26));
            wrapSerializer.serialize(reused, ds);

            din.setBuffer(ds.getSharedBuffer());
            IterationRecord<byte[]> rs = wrapSerializer.deserialize(din);
            totalLength += rs.getValue().length;
        }
        now += System.nanoTime();
        System.out.println("time used " + now / 1e6 + "ms");
        System.out.println("total length " + totalLength);
    }
}
