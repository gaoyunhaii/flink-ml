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

package org.apache.flink.iteration.feedback;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.typeinfo.ReusedIterationRecordSerializer;
import org.apache.flink.statefun.flink.core.feedback.FeedbackChannel;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SerializedFeedbackChannelTest {

    private static final Object FIN_TAG = new Object();

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
    public void testWriteAndRead() throws InterruptedException {
        ReusedIterationRecordSerializer<byte[]> serializer =
                new ReusedIterationRecordSerializer<>(BytePrimitiveArraySerializer.INSTANCE);

        FeedbackConfiguration config =
                new FeedbackConfiguration(new Configuration(), new String[] {"/tmp"});
        SerializedFeedbackChannel<IterationRecord<byte[]>> channel =
                new SerializedFeedbackChannel<>(config, serializer);

        Writer writer = new Writer(COUNT, ARRAY_SIZE, channel);
        Reader reader = new Reader();

        writer.getQueue().add((Runnable) () -> channel.registerProducer(writer.getQueue()::add));
        reader.getQueue()
                .add((Runnable) () -> channel.registerConsumer(reader, reader.getQueue()::add));

        long now = -System.nanoTime();
        Thread readThread = new Thread(reader);
        readThread.start();
        Thread writeThread = new Thread(writer);
        writeThread.start();

        readThread.join();
        writer.getQueue().add(FIN_TAG);
        writeThread.join();
        now += System.nanoTime();
        System.out.println("Time used " + now / 1e6 + "ms");
    }

    private abstract static class PoorMailboxExecutor implements Runnable {

        private final ConcurrentLinkedQueue<Object> queue;

        private PoorMailboxExecutor() {
            this.queue = new ConcurrentLinkedQueue<>();
        }

        public ConcurrentLinkedQueue<Object> getQueue() {
            return queue;
        }

        public abstract void defaultAction();

        @Override
        public void run() {
            loop:
            while (true) {
                while (true) {
                    Object next = queue.poll();
                    if (next == null) {
                        break;
                    }

                    if (next instanceof Runnable) {
                        ((Runnable) next).run();
                    } else if (next == FIN_TAG) {
                        break loop;
                    } else {
                        throw new RuntimeException("Not knowing... " + next);
                    }
                }

                defaultAction();
            }
        }
    }

    private static class Writer extends PoorMailboxExecutor {

        final int count;

        final int arraySize;

        final FeedbackChannel<IterationRecord<byte[]>> channel;

        final IterationRecord<byte[]> reused;

        final Random random;

        int next;

        public Writer(
                int count,
                int arraySize,
                SerializedFeedbackChannel<IterationRecord<byte[]>> channel) {
            this.count = count;
            this.arraySize = arraySize;
            this.channel = channel;
            this.reused = IterationRecord.newRecord(null, 0);
            random = new Random();
        }

        @Override
        public void defaultAction() {
            try {
                if (next >= count) {
                    return;
                }

                int nextBatch = random.nextInt(50) + 1;
                for (int i = 0; i < nextBatch && next < COUNT; ++i) {
                    reused.setValue(ARRAYS.get(next % 26));
                    channel.put(reused);
                    next++;

                    //                    if (next % 10000 == 0) {
                    //                        System.out.println("next to write is " + next);
                    //                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class Reader extends PoorMailboxExecutor
            implements FeedbackConsumer<IterationRecord<byte[]>> {

        int next;

        @Override
        public void defaultAction() {
            // do nothing
        }

        @Override
        public void processFeedback(IterationRecord<byte[]> element) throws Exception {
            if (!Arrays.equals(ARRAYS.get(next % 26), element.getValue())) {
                System.out.println(next + " is not equals!!! ");
                throw new RuntimeException("bad bad");
            }
            next++;

            //            if (next % 10000 == 0) {
            //                System.out.println("next to read is " + next);
            //            }

            if (next >= COUNT) {
                getQueue().add(FIN_TAG);
            }
        }
    }
}
