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

package org.apache.flink.iteration.feedback.ring;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.iteration.feedback.ReadView;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RingQueueTest {

    @Test
    public void testWriteAndRead() throws IOException {
        List<byte[]> records = new ArrayList<>();
        int count = 1000000;

        for (int i = 0; i < count; ++i) {
            records.add(RandomUtils.nextBytes(128 * 5));
        }
        List<byte[]> result = new ArrayList<>(count);

        ByteBuffer buffer = ByteBuffer.allocate(1024 * 512);
        RingQueue ringQueue = new RingQueue(buffer);

        BytePrimitiveArraySerializer serializer = BytePrimitiveArraySerializer.INSTANCE;
        DataOutputSerializer outputView = new DataOutputSerializer(256);

        long used = -System.nanoTime();
        readAndWrite(records, 0, count / 3, result, ringQueue, outputView, serializer);
        readAndWrite(records, count / 3, count / 3 * 2, result, ringQueue, outputView, serializer);
        readAndWrite(records, count / 3 * 2, count, result, ringQueue, outputView, serializer);
        used += System.nanoTime();
        System.out.println("Used = " + used / 1e6 + "ms");

        assertEquals(records.size(), result.size());
        for (int i = 0; i < records.size(); ++i) {
            assertArrayEquals(records.get(i), result.get(i));
        }
    }

    private void readAndWrite(
            List<byte[]> records,
            int start,
            int end,
            List<byte[]> result,
            RingQueue ringQueue,
            DataOutputSerializer outputView,
            TypeSerializer<byte[]> serializer)
            throws IOException {

        int next = start;
        while (next < end) {
            outputView.setPositionUnsafe(0);
            serializer.serialize(records.get(next), outputView);
            ByteBuffer byteBuffer = outputView.wrapAsByteBuffer();

            boolean succeed = ringQueue.put(byteBuffer);
            if (!succeed) {
                ReadView readView = ringQueue.getReadView();
                DataInputView view = readView.toDataInputView();
                System.out.println("view: " + readView);
                while (true) {
                    try {
                        byte[] t = serializer.deserialize(view);
                        result.add(t);
                    } catch (EOFException e) {
                        break;
                    }
                }
                System.out.println("Read size " + result.size());
                readView.recycle();
                succeed = ringQueue.put(byteBuffer);
            }
            assertTrue(succeed);

            next += 1;
        }

        ReadView readView = ringQueue.getReadView();
        DataInputView view = readView.toDataInputView();
        System.out.println("view: " + readView);
        while (true) {
            try {
                byte[] t = serializer.deserialize(view);
                result.add(t);
            } catch (EOFException e) {
                break;
            }
        }
        readView.recycle();
        System.out.println("Read size " + result.size());
    }
}
