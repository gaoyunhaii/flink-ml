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

package org.apache.flink.statefun.flink.core.feedback;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.config.IterationOptions;
import org.apache.flink.iteration.feedback.FeedbackConfiguration;
import org.apache.flink.iteration.feedback.SerializedFeedbackChannel;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.streaming.api.graph.StreamConfig;

import java.io.Serializable;
import java.util.concurrent.Executor;

/** Provider */
public interface IterationFeedbackChannelProvider extends Serializable {

    <X> FeedbackChannel<X> getProducerChannel(
            Configuration configuration,
            String[] spillingDirectoriesPaths,
            StreamConfig config,
            IterationID iterationId,
            int feedbackIndex,
            int indexOfThisSubtask,
            int attemptNum,
            Executor producerExecutor);

    void registerConsumerToChannel(
            Configuration configuration,
            String[] spillingDirectoriesPaths,
            StreamConfig config,
            IterationID iterationId,
            int feedbackIndex,
            int indexOfThisSubtask,
            int attemptNum,
            Executor consumerExecutor,
            FeedbackConsumer<IterationRecord<?>> consumer);

    default <T> FeedbackChannel<T> createFeedbackChannel(
            Configuration configuration,
            String[] spillingDirectoriesPaths,
            StreamConfig config,
            IterationID iterationId,
            int feedbackIndex,
            int indexOfThisSubtask,
            int attemptNum) {
        FeedbackKey<T> feedbackKey = OperatorUtils.createFeedbackKey(iterationId, feedbackIndex);
        SubtaskFeedbackKey<T> key = feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);

        FeedbackChannelBroker broker = FeedbackChannelBroker.get();

        IterationOptions.FeedbackType feedbackType =
                configuration.get(IterationOptions.FEEDBACK_CHANNEL_TYPE);

        FeedbackChannel<T> channel;
        if (feedbackType.equals(IterationOptions.FeedbackType.RECORD)) {
            channel = broker.getChannel(key, RecordBasedFeedbackChannel::new);
        } else {
            FeedbackConfiguration feedbackConfiguration =
                    new FeedbackConfiguration(configuration, spillingDirectoriesPaths);
            channel =
                    broker.getChannel(
                            key,
                            (ignored) ->
                                    new SerializedFeedbackChannel<>(
                                            feedbackConfiguration,
                                            config.getTypeSerializerIn(
                                                    0,
                                                    IterationFeedbackChannelProvider.class
                                                            .getClassLoader())));
        }

        return channel;
    }
}
