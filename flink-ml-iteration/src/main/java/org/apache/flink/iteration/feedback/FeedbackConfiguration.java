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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.config.IterationOptions;
import org.apache.flink.iteration.operator.OperatorUtils;

/** */
public class FeedbackConfiguration {

    private final MemorySize inMemoryBufferSize;

    private final MemorySize fileWriteBufferSize;

    private final MemorySize fileReadBufferSize;

    private final Path basePath;

    public FeedbackConfiguration(Configuration configuration, String[] localSpillPaths) {
        this.inMemoryBufferSize =
                configuration.get(IterationOptions.FEEDBACK_IN_MEMORY_BUFFER_SIZE);
        this.fileWriteBufferSize = configuration.get(IterationOptions.FEEDBACK_WRITE_BUFFER_SIZE);
        this.fileReadBufferSize = configuration.get(IterationOptions.FEEDBACK_READ_BUFFER_SIZE);
        this.basePath = OperatorUtils.getDataCachePath(configuration, localSpillPaths);
    }

    public MemorySize getInMemoryBufferSize() {
        return inMemoryBufferSize;
    }

    public MemorySize getFileWriteBufferSize() {
        return fileWriteBufferSize;
    }

    public MemorySize getFileReadBufferSize() {
        return fileReadBufferSize;
    }

    public Path getBasePath() {
        return basePath;
    }
}
