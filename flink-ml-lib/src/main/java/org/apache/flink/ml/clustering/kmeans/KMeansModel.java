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

package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A Model which clusters data into k clusters using the model data computed by {@link KMeans}. */
public class KMeansModel implements Model<KMeansModel>, KMeansParams<KMeansModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private Table centroidsTable;

    public KMeansModel() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public void setModelData(Table... inputs) {
        centroidsTable = inputs[0];
    }

    @Override
    public Table[] getModelData() {
        return new Table[] {centroidsTable};
    }

    @Override
    public Table[] transform(Table... inputs) {
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<DenseVector[]> centroids =
                tEnv.toDataStream(centroidsTable)
                        .map(
                                new MapFunction<Row, DenseVector[]>() {
                                    @Override
                                    public DenseVector[] map(Row row) {
                                        return (DenseVector[]) row.getField("f0");
                                    }
                                });

        String featureCol = getFeaturesCol();
        String predictionCol = getPredictionCol();
        DistanceMeasure distanceMeasure = DistanceMeasure.getInstance(getDistanceMeasure());

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputs[0].getResolvedSchema());
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.INT),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), predictionCol));

        DataStream<Row> input = tEnv.toDataStream(inputs[0]);
        DataStream<Row> output =
                input.connect(centroids.broadcast())
                        .transform(
                                "SelectNearestCentroid",
                                outputTypeInfo,
                                new SelectNearestCentroidOperator(featureCol, distanceMeasure));

        return new Table[] {tEnv.fromDataStream(output)};
    }

    private static class SelectNearestCentroidOperator extends AbstractStreamOperator<Row>
            implements TwoInputStreamOperator<Row, DenseVector[], Row> {
        private ListState<Row> inputs;
        // TODO: use broadcast state here.
        private ListState<DenseVector[]> centroids;

        private final String featureCol;
        private final DistanceMeasure distanceMeasure;

        public SelectNearestCentroidOperator(String featureCol, DistanceMeasure distanceMeasure) {
            this.featureCol = featureCol;
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            inputs =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("points", Row.class));
            TypeInformation<DenseVector[]> type =
                    ObjectArrayTypeInfo.getInfoFor(TypeInformation.of(DenseVector.class));
            centroids =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("centroids", type));
        }

        @Override
        public void processElement1(StreamRecord<Row> streamRecord) throws Exception {
            // TODO: output result once the broadcast input has ended.
            inputs.add(streamRecord.getValue());
        }

        @Override
        public void processElement2(StreamRecord<DenseVector[]> streamRecord) throws Exception {
            centroids.add(streamRecord.getValue());
        }

        @Override
        public void finish() throws Exception {
            List<DenseVector[]> list = IteratorUtils.toList(centroids.get().iterator());
            if (list.size() != 1) {
                throw new RuntimeException(
                        "The operator received "
                                + list.size()
                                + " list of centroids in this round");
            }
            DenseVector[] centroidValues = list.get(0);

            for (Row input : inputs.get()) {
                DenseVector point = (DenseVector) input.getField(featureCol);

                double minDistance = Double.MAX_VALUE;
                int closestCentroidId = -1;

                for (int i = 0; i < centroidValues.length; i++) {
                    DenseVector centroid = centroidValues[i];
                    double distance = distanceMeasure.distance(centroid, point);
                    if (distance < minDistance) {
                        minDistance = distance;
                        closestCentroidId = i;
                    }
                }

                // TODO: add and check the prediction column name.
                output.collect(new StreamRecord<>(Row.join(input, Row.of(closestCentroidId))));
            }
            inputs.clear();
            centroids.clear();
        }
    }

    @Override
    public void save(String path) throws IOException {
        // TODO: save model data.
        ReadWriteUtils.saveMetadata(this, path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    public static KMeansModel load(String path) throws IOException {
        // TODO: load model data.
        return ReadWriteUtils.loadStageParam(path);
    }
}
