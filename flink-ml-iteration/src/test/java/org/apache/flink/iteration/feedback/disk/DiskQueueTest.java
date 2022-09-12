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

package org.apache.flink.iteration.feedback.disk;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.iteration.feedback.ReadView;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DiskQueueTest {

    @Test
    public void testWriteAndRead() throws Exception {
        List<byte[]> records = new ArrayList<>();
        int count = 1000000;

        for (int i = 0; i < count; ++i) {
            records.add(RandomUtils.nextBytes(128 * 5));
        }
        List<byte[]> result = new ArrayList<>(count);

        ExecutorService ioExecutor = Executors.newSingleThreadExecutor();

        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024 * 2);
        DiskQueue diskQueue = new DiskQueue(buffer, ioExecutor, "/tmp/tmp.spill");

        BytePrimitiveArraySerializer serializer = BytePrimitiveArraySerializer.INSTANCE;
        DataOutputSerializer outputView = new DataOutputSerializer(256);

        long now = -System.nanoTime();
        for (int i = 0; i < count; ++i) {
            outputView.setPositionUnsafe(0);
            serializer.serialize(records.get(i), outputView);
            ByteBuffer byteBuffer = outputView.wrapAsByteBuffer();

            diskQueue.put(byteBuffer);
        }
        diskQueue.close();
        now += System.nanoTime();
        System.out.println("Writting used " + now / 1e6 + "ms");

        now = -System.nanoTime();
        ByteBuffer readBuffer = ByteBuffer.allocate(1024 * 1024 * 2);
        ReadView readView = diskQueue.getReadView(readBuffer);
        DataInputView view = readView.toDataInputView();

        while (true) {
            try {
                byte[] t = serializer.deserialize(view);
                result.add(t);
            } catch (EOFException e) {
                break;
            }
        }
        readView.recycle();
        now += System.nanoTime();
        System.out.println("reading used " + now / 1e6 + "ms");

        assertEquals(records.size(), result.size());
        for (int i = 0; i < records.size(); ++i) {
            assertArrayEquals(records.get(i), result.get(i));
        }
    }
}
