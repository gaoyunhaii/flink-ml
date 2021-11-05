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

package org.apache.flink.ml.common.iteration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.util.Collector;

/**
 * If an instance of this class is used as the termination criteria AND the iteration body is
 * executed in sync mode, the iteration body will be executed for at most the given number of
 * rounds.
 *
 * <p>TODO: explain the sync mode here.
 *
 * @param <T> Input value type.
 */
public class TerminateOnMaxIterationNum<T>
        implements FlatMapFunction<T, Integer>, IterationListener<Integer> {
    private final int numRounds;

    public TerminateOnMaxIterationNum(int numRounds) {
        this.numRounds = numRounds;
    }

    @Override
    public void flatMap(T integer, Collector<Integer> collector) {}

    @Override
    public void onEpochWatermarkIncremented(
            int epochWatermark, Context context, Collector<Integer> out) {
        if (epochWatermark <= numRounds - 2) {
            out.collect(0);
        }
    }

    @Override
    public void onIterationTerminated(Context context, Collector<Integer> collector) {}
}
