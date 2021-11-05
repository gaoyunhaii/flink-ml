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

package org.apache.flink.ml.clustering;

import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.LinkedList;
import java.util.List;

/** Provides the default data sets used for the k-means tests. */
public class KMeansData {
    public static final double[][] POINTS =
            new double[][] {
                new double[] {0.0, 1.0},
                new double[] {0.0, 2.0},
                new double[] {0.0, 3.0},
                new double[] {10.0, 1.0},
                new double[] {10.0, 3.0},
                new double[] {20.0, 1.0},
                new double[] {20.0, 2.0},
                new double[] {20.0, 3.0}
            };

    public static Table getData(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        List<DenseVector> features = new LinkedList<>();
        for (double[] point : POINTS) {
            features.add(new DenseVector(point));
        }
        Schema schema = Schema.newBuilder().column("f0", DataTypes.of(DenseVector.class)).build();
        return tEnv.fromDataStream(env.fromCollection(features), schema).as("features");
    }
}
