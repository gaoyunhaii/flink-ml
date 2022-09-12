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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.iteration.feedback.disk.DiskQueue;
import org.apache.flink.iteration.feedback.ring.RingQueue;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Some implementation. */
public class FeedbackChannel<T> {

    private final FeedbackConfiguration config;

    private final TypeSerializer<T> typeSerializer;

    private final Executor writerExecutor;

    private final Executor readerExecutor;

    private final FeedbackConsumer<T> consumer;

    private final ByteBuffer fileWriteBuffer;

    private final ByteBuffer fileReadBuffer;

    private final ExecutorService ioExecutor;

    private final SerializationDelegate<T> ioReadableWritable;

    private final DataOutputSerializer serializer;

    private final RingQueue inMemQueue;

    private DiskQueue currentDiskQueue;

    private boolean isReading;

    private final List<DataInputView> pendingReads = new ArrayList<>();

    public FeedbackChannel(
            FeedbackConfiguration config,
            TypeSerializer<T> typeSerializer,
            Executor writerExecutor,
            Executor readerExecutor,
            FeedbackConsumer<T> consumer) {
        this.config = checkNotNull(config);
        this.typeSerializer = checkNotNull(typeSerializer);
        this.writerExecutor = checkNotNull(writerExecutor);
        this.readerExecutor = checkNotNull(readerExecutor);
        this.consumer = checkNotNull(consumer);

        ByteBuffer inMemoryBuffer =
                ByteBuffer.allocate((int) config.getInMemoryBufferSize().getBytes());
        this.fileWriteBuffer =
                ByteBuffer.allocate((int) config.getFileWriteBufferSize().getBytes());
        this.fileReadBuffer = ByteBuffer.allocate((int) config.getFileReadBufferSize().getBytes());
        this.ioExecutor = Executors.newSingleThreadExecutor();

        this.ioReadableWritable = new SerializationDelegate<>(typeSerializer);
        this.serializer = new DataOutputSerializer(128);

        this.inMemQueue = new RingQueue(inMemoryBuffer);
    }

    public void put(T t) throws Exception {
        ioReadableWritable.setInstance(t);
        ByteBuffer serializedBuffer = RecordWriter.serializeRecord(serializer, ioReadableWritable);

        if (serializedBuffer.remaining()
                > Math.min(inMemQueue.getBuffer().capacity(), fileWriteBuffer.capacity())) {
            // TODO: Write to a separate file
        }

        if (currentDiskQueue != null) {
            // The in-mem queue is full and not been consumed yet.
            // In this case, we just bookkeeping the record.
            currentDiskQueue.put(serializedBuffer);
            return;
        }

        boolean succeed = inMemQueue.put(serializedBuffer);
        if (!succeed) {
            currentDiskQueue = null;
        }

        if (succeed && !isReading) {
            RingQueue.ReadView readView = inMemQueue.getReadView();
            isReading = true;
            readerExecutor.execute(
                    () -> {
                        // TODO: consume readView
                    });
        }
    }
}
