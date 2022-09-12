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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.iteration.feedback.PagedInputView;
import org.apache.flink.iteration.feedback.ReadView;
import org.apache.flink.util.function.SupplierWithException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkState;

/** A ring queue... */
public class RingQueue {

    private final ByteBuffer buffer;

    private int readStart;

    private int readingSize;

    private int writtenSize;

    public RingQueue(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int getWrittenSize() {
        return writtenSize;
    }

    public boolean put(ByteBuffer serialized) {
        int serializedSize = serialized.remaining();
        int remaining = buffer.capacity() - readingSize - writtenSize;

        if (remaining < serializedSize) {
            return false;
        }

        // let's copy it in
        int writeStartPos = (readStart + readingSize + writtenSize) % buffer.capacity();
        int firstPartSize = Math.min(serializedSize, buffer.capacity() - writeStartPos);
        serialized.get(buffer.array(), writeStartPos, firstPartSize);

        int secondPartSize = serialized.remaining();
        if (secondPartSize > 0) {
            serialized.get(buffer.array(), 0, secondPartSize);
        }

        writtenSize += serializedSize;
        return true;
    }

    public RingBufferReadView getReadView() {
        int currentWritingStartPos = (readStart + readingSize) % buffer.capacity();
        int currentWritingEndPos = (currentWritingStartPos + writtenSize) % buffer.capacity();

        readingSize += writtenSize;
        writtenSize = 0;
        return new RingBufferReadView(currentWritingStartPos, currentWritingEndPos);
    }

    private void recycle(RingBufferReadView readView) {
        checkState(readView != null && readView.start == readStart);

        int size =
                (readView.start < readView.endExclusive)
                        ? (readView.endExclusive - readView.start)
                        : (buffer.capacity() - readView.start + readView.endExclusive);
        readStart = readView.endExclusive;
        readingSize -= size;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public class RingBufferReadView implements ReadView {

        private final int start;

        private final int endExclusive;

        public RingBufferReadView(int start, int endExclusive) {
            this.start = start;
            this.endExclusive = endExclusive;
        }

        public DataInputView toDataInputView() throws IOException {
            ByteBuffer[] buffers = toBuffers();
            return new PagedInputView(
                    new SupplierWithException<ByteBuffer, IOException>() {

                        private int next = 0;

                        @Override
                        public ByteBuffer get() throws IOException {
                            if (next < buffers.length) {
                                return buffers[next++];
                            }

                            throw new EOFException();
                        }
                    });
        }

        @Override
        public void recycle() {
            RingQueue.this.recycle(this);
        }

        public void writeTo(FileChannel channel) throws IOException {
            for (ByteBuffer buffer : toBuffers()) {
                int remaining = buffer.remaining();
                do {
                    remaining -= channel.write(buffer);
                } while (remaining > 0);
            }
        }

        private ByteBuffer[] toBuffers() {
            if (start <= endExclusive) {
                return new ByteBuffer[] {
                    ByteBuffer.wrap(buffer.array(), start, endExclusive - start)
                };
            } else {
                return new ByteBuffer[] {
                    ByteBuffer.wrap(buffer.array(), start, buffer.capacity() - start),
                    ByteBuffer.wrap(buffer.array(), 0, endExclusive)
                };
            }
        }

        @Override
        public String toString() {
            return "RingBufferReadView{"
                    + "start="
                    + start
                    + ", endExclusive="
                    + endExclusive
                    + '}';
        }
    }
}
