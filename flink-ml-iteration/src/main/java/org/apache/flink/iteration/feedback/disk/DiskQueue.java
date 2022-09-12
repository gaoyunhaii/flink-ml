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

import org.apache.flink.iteration.feedback.ring.RingQueue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.Preconditions.checkState;

/** Writing to the disk first */
public class DiskQueue {

    private static final double FLUSH_THRESHOLD = 0.4;

    private final RingQueue writeBuffer;

    private final String path;

    private final BlockingQueue<Object> flushTasks;

    private final BlockingQueue<Object> finishedTasks;

    private boolean closed;

    public DiskQueue(ByteBuffer buffer, ExecutorService ioExecutor, String path)
            throws IOException {
        this.writeBuffer = new RingQueue(buffer);
        this.path = path;

        this.flushTasks = new LinkedBlockingQueue<>();
        this.finishedTasks = new LinkedBlockingQueue<>();

        ioExecutor.submit(new FlushRunnable(flushTasks, finishedTasks, path));
    }

    public boolean put(ByteBuffer serialized) throws Exception {
        checkState(serialized.remaining() < writeBuffer.getBuffer().capacity());

        while (!writeBuffer.put(serialized)) {
            waitTillNextFlushFinished();
        }

        if (writeBuffer.getWrittenSize() > FLUSH_THRESHOLD * writeBuffer.getBuffer().capacity()) {
            flushTasks.add(writeBuffer.getReadView());
        }

        return true;
    }

    public void close() throws Exception {
        if (closed) {
            return;
        }

        // Flush the remaining bytes
        flushTasks.add(writeBuffer.getReadView());

        // The finish tag
        Object tag = new Object();
        flushTasks.add(tag);

        // Now wait till all done
        Object next;
        do {
            next = waitTillNextFlushFinished();
        } while (next != tag);

        closed = true;
    }

    public DiskQueueReadView getReadView(ByteBuffer readBuffer) throws IOException {
        return new DiskQueueReadView(readBuffer, path);
    }

    private Object waitTillNextFlushFinished() throws Exception {
        Object next = finishedTasks.take();
        if (next instanceof Exception) {
            throw (Exception) next;
        } else if (next instanceof RingQueue.RingBufferReadView) {
            ((RingQueue.RingBufferReadView) next).recycle();
        }

        return next;
    }

    public static class FlushRunnable implements Runnable {

        private final BlockingQueue<Object> tasks;

        private final BlockingQueue<Object> resultQueue;

        private final FileChannel fileChannel;

        public FlushRunnable(
                BlockingQueue<Object> flushTasks, BlockingQueue<Object> resultQueue, String path)
                throws IOException {
            this.tasks = flushTasks;
            this.resultQueue = resultQueue;
            this.fileChannel =
                    FileChannel.open(
                            new File(path).toPath(),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Object next = tasks.take();
                    if (!(next instanceof RingQueue.RingBufferReadView)) {
                        resultQueue.add(next);
                        break;
                    }

                    RingQueue.RingBufferReadView view = (RingQueue.RingBufferReadView) next;
                    view.writeTo(fileChannel);
                } catch (Exception e) {
                    resultQueue.add(e);
                    break;
                } finally {
                    try {
                        fileChannel.close();
                    } catch (IOException e) {
                        resultQueue.add(e);
                    }
                }
            }
        }
    }
}
