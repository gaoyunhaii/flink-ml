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

package org.apache.flink.iteration.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;

import static org.apache.flink.configuration.ConfigOptions.key;

/** The options for the iteration. */
public class IterationOptions {

    public static final ConfigOption<String> DATA_CACHE_PATH =
            key("iteration.data-cache.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The base path of the data cached used inside the iteration. "
                                    + "If not specified, it will use local path randomly chosen from "
                                    + CoreOptions.TMP_DIRS.key());

    public static final ConfigOption<MemorySize> FEEDBACK_IN_MEMORY_BUFFER_SIZE =
            key("iteration.feedback.in-memory.buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(4))
                    .withDescription("The total size of in-memory queue");

    public static final ConfigOption<MemorySize> FEEDBACK_WRITE_BUFFER_SIZE =
            key("iteration.feedback.write.buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(1))
                    .withDescription("Write buffer size");

    public static final ConfigOption<MemorySize> FEEDBACK_READ_BUFFER_SIZE =
            key("iteration.feedback.read.buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(1))
                    .withDescription("Read buffer size");

    public static final ConfigOption<FeedbackType> FEEDBACK_CHANNEL_TYPE =
            key("iteration.feedback.channel-type")
                    .enumType(FeedbackType.class)
                    .defaultValue(FeedbackType.SERIALIZED)
                    .withDescription("feedback channel type");

    public static enum FeedbackType {
        RECORD,

        SERIALIZED
    }
}
