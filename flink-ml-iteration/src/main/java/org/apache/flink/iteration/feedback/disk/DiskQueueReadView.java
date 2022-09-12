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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.iteration.feedback.PagedInputView;
import org.apache.flink.iteration.feedback.ReadView;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class DiskQueueReadView implements ReadView {

    private final ByteBuffer readBuffer;

    private final String path;

    private final FileChannel readChannel;

    public DiskQueueReadView(ByteBuffer readBuffer, String path) throws IOException {
        this.readBuffer = readBuffer;
        this.path = path;

        readChannel = FileChannel.open(new File(path).toPath(), StandardOpenOption.READ);
    }

    @Override
    public DataInputView toDataInputView() throws IOException {
        return new PagedInputView(
                () -> {
                    if (readChannel.position() >= readChannel.size()) {
                        throw new EOFException();
                    }

                    readChannel.read(readBuffer);
                    readBuffer.flip();
                    return readBuffer;
                });
    }

    @Override
    public void recycle() throws IOException {
        readChannel.close();
        new File(path).delete();
    }
}
