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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.iteration.feedback.disk.DiskQueue;
import org.apache.flink.iteration.feedback.ring.RingQueue;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The feedback channel.
 *
 * <p>It switches between two states: 1. Writing to in-mem queue. 2. Writing to on-disk queue.
 *
 * <p>The data is first writing to the in-mem queue. Meanwhile, The reader wills start reading from
 * the in-mem queue. Everytime it is done, it will read from the next piece of data.
 *
 * <p>Once the writer found that the in-mem queue is full, it will write to disk instead. In this
 * case, the writer will keep writing to disk until the whole in-mem queue is consumed. After that,
 * the writer will write to the in-mem queue, and the reader will first read the disk spills.
 *
 * <p>It could be seen that there should at most one in-memory read-view and one disk view at the
 * same time.
 *
 * <p>At last, the large record is dealt separately.
 */
public class SerializedFeedbackChannel<T> {

    private final FeedbackConfiguration config;

    private final TypeSerializer<T> typeSerializer;

    private final Executor writerExecutor;

    private final ByteBuffer fileWriteBuffer;

    private final ByteBuffer fileReadBuffer;

    private final ExecutorService flushExecutor;

    // -----  serialization related ------------------
    private final DataOutputSerializer serializer;

    private final SerializationDelegate<T> ioReadableWritable;

    // ------- queues & read views -------------------

    private final RingQueue inMemQueue;

    @Nullable private DiskQueue currentDiskQueue;

    //  ------------ consumer state ----------------------------

    private boolean isReading = false;

    private final ConcurrentLinkedQueue<ReadView> pendingReadViews = new ConcurrentLinkedQueue<>();

    private final AtomicReference<ConsumerTask> consumerRef = new AtomicReference<>();

    public SerializedFeedbackChannel(
            FeedbackConfiguration config,
            TypeSerializer<T> typeSerializer,
            Executor writerExecutor) {
        this.config = checkNotNull(config);
        this.typeSerializer = checkNotNull(typeSerializer);
        this.writerExecutor = checkNotNull(writerExecutor);

        ByteBuffer inMemoryBuffer =
                ByteBuffer.allocate((int) config.getInMemoryBufferSize().getBytes());
        this.fileWriteBuffer =
                ByteBuffer.allocate((int) config.getFileWriteBufferSize().getBytes());
        this.fileReadBuffer = ByteBuffer.allocate((int) config.getFileReadBufferSize().getBytes());
        this.flushExecutor = Executors.newSingleThreadExecutor();

        this.ioReadableWritable = new SerializationDelegate<>(typeSerializer);
        this.serializer = new DataOutputSerializer(128);

        this.inMemQueue = new RingQueue(inMemoryBuffer);
    }

    public void put(T t) throws Exception {
        ioReadableWritable.setInstance(t);
        ByteBuffer serializedBuffer = RecordWriter.serializeRecord(serializer, ioReadableWritable);

        if (serializedBuffer.remaining()
                > Math.min(inMemQueue.getBuffer().capacity(), fileWriteBuffer.capacity())) {
            throw new RuntimeException("Large record not supported yet");
        }

        if (currentDiskQueue != null) {
            // The in-mem queue is full and not been consumed yet.
            // In this case, we just bookkeeping the record.
            currentDiskQueue.put(serializedBuffer);
            return;
        }

        boolean succeed = inMemQueue.put(serializedBuffer);
        if (!succeed) {
            checkState(
                    currentDiskQueue == null,
                    "There should be no disk queue if we are writing to the memory");
            currentDiskQueue =
                    new DiskQueue(
                            fileWriteBuffer,
                            flushExecutor,
                            new Path(config.getBasePath(), UUID.randomUUID().toString() + ".spill")
                                    .toString());
            currentDiskQueue.put(serializedBuffer);
        }

        if (!isReading && inMemQueue.getWrittenSize() > 0 && consumerRef.get() != null) {
            // If not reading and in-memory queue is not empty, we need to drain the in-memory
            // queue.
            // We do not notify on-disk queue here. It could be reading only after the memory is
            // drained.

            pendingReadViews.add(inMemQueue.getReadView());
            consumerRef.get().drain();
            isReading = true;
        }
    }

    public void registerConsumer(FeedbackConsumer<T> consumer, Executor executor) {
        ConsumerTask consumerTask = new ConsumerTask(executor, consumer);

        if (!this.consumerRef.compareAndSet(null, consumerTask)) {
            throw new IllegalStateException(
                    "There can be only a single consumer in a FeedbackChannel.");
        }

        // Try to drain the existing records
        onConsumed(Collections.emptyList());
    }

    public void close() {
        // do nothing currently
    }

    private void onConsumed(List<ReadView> consumed) {
        writerExecutor.execute(
                () -> {
                    try {
                        // Recycle the finished view.
                        for (ReadView readView : consumed) {
                            readView.recycle();
                        }

                        // The in-mem queue always comes before the on-disk queue.
                        // Before the in-mem queue is fully drained, users has to
                        // continue to write to the disk.
                        if (inMemQueue.getWrittenSize() > 0) {
                            pendingReadViews.add(inMemQueue.getReadView());
                        } else if (currentDiskQueue != null) {
                            currentDiskQueue.close();
                            pendingReadViews.add(currentDiskQueue.getReadView(fileReadBuffer));
                            currentDiskQueue = null;
                        }

                        checkState(isReading, "Must be still isReading");
                        if (pendingReadViews.size() > 0) {
                            consumerRef.get().drain();
                        } else {
                            isReading = false;
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private class ConsumerTask implements Runnable {

        private final Executor readerExecutor;

        private final FeedbackConsumer<T> consumer;

        private ConsumerTask(Executor readerExecutor, FeedbackConsumer<T> consumer) {
            this.readerExecutor = readerExecutor;
            this.consumer = consumer;
        }

        public void drain() {
            readerExecutor.execute(this);
        }

        @Override
        public void run() {
            List<ReadView> consumed = new ArrayList<>();

            while (true) {
                ReadView view = pendingReadViews.poll();
                if (view == null) {
                    break;
                }

                try {
                    DataInputView dataInputView = view.toDataInputView();
                    while (true) {
                        try {
                            T t = typeSerializer.deserialize(dataInputView);
                            consumer.processFeedback(t);
                        } catch (EOFException e) {
                            break;
                        } catch (Exception exception) {
                            throw new RuntimeException(exception);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                consumed.add(view);
            }

            onConsumed(consumed);
        }
    }
}
