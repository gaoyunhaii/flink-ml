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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Tests KMeans and KMeansModel. */
public class KMeansTest extends AbstractTestBase {

    private static class RowFormat extends SimpleStreamFormat<Row> {
        @Override
        public Reader<Row> createReader(Configuration config, FSDataInputStream stream) {
            return new Reader<Row>() {
                private final Kryo kryo = new Kryo();
                private final Input input = new Input(stream);

                @Override
                public Row read() throws IOException {
                    if (input.eof()) {
                        return null;
                    }
                    return kryo.readObject(input, Row.class);
                }

                @Override
                public void close() throws IOException {
                    stream.close();
                }
            };
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return TypeInformation.of(Row.class);
        }
    }

    private static class IntegerFormat extends SimpleStreamFormat<Integer> {
        @Override
        public Reader<Integer> createReader(Configuration config, FSDataInputStream stream) {
            return new Reader<Integer>() {
                private final Kryo kryo = new Kryo();
                private final Input input = new Input(stream);

                @Override
                public Integer read() throws IOException {
                    if (input.eof()) {
                        return null;
                    }
                    return kryo.readObject(input, Integer.class);
                }

                @Override
                public void close() throws IOException {
                    stream.close();
                }
            };
        }

        @Override
        public TypeInformation<Integer> getProducedType() {
            return TypeInformation.of(Integer.class);
        }
    }

    private static class IntegerEncoder implements Encoder<Integer> {
        @Override
        public void encode(Integer value, OutputStream outputStream) throws IOException {
            System.out.println("flag.........encoder " + value);
            Kryo kryo = new Kryo();
            Output output = new Output(outputStream);
            kryo.writeObject(output, value);
            output.flush();
        }
    }

    //    @Test
    public void testSaveLoad() throws Exception {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(100);

        String tempDir = Files.createTempDirectory("").toString();

        FileSink<Integer> sink =
                FileSink.forRowFormat(new Path(tempDir), new IntegerEncoder())
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .build();

        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5)).sinkTo(sink);

        env.execute();

        System.out.println("Successfully produced values to the directory " + tempDir);

        File[] files = new File(tempDir).listFiles();

        Path[] paths = new Path[files.length];
        for (int i = 0; i < paths.length; i++) {
            System.out.println("flag...........testLoad " + files[i].getAbsolutePath());
            paths[i] = Path.fromLocalFile(files[i]);
        }

        Source<Integer, ?, ?> source =
                FileSource.forRecordStreamFormat(new IntegerFormat(), paths).build();
        DataStream<Integer> input =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "input");
        input.print();

        env.execute();
    }

    @Test
    public void testCheckpointWithMetadata() throws Exception {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.of(Integer.class))
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .build();

        Table data = tEnv.fromDataStream(env.fromCollection(Arrays.asList(1, 2, 3)), schema);
        tEnv.toDataStream(data).print();

        env.execute();
    }

    @Test
    public void testKMeansDataStream() throws Exception {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(100);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table data = KMeansData.getData(env);
        KMeans kmeans = new KMeans().setMaxIter(10).setK(2);
        KMeansModel model = kmeans.fit(data);

        System.out.println("flag......execute");

        env.execute();
    }

    private static double computeCost(
            List<Tuple2<DenseVector, Integer>> pointsWithCentroids,
            DistanceMeasure distanceMeasure) {
        Map<Integer, List<DenseVector>> pointsByClusterId =
                pointsWithCentroids.stream()
                        .collect(
                                Collectors.groupingBy(
                                        t -> t.f1,
                                        Collectors.mapping(t -> t.f0, Collectors.toList())));

        System.out.println("pointsByClusterId " + pointsByClusterId);
        double loss = 0;
        for (Map.Entry<Integer, List<DenseVector>> entry : pointsByClusterId.entrySet()) {
            DenseVector meanPoint = getMeanPoint(entry.getValue());
            for (DenseVector point : entry.getValue()) {
                loss += distanceMeasure.distance(meanPoint, point);
            }
        }
        return loss;
    }

    private static DenseVector getMeanPoint(List<DenseVector> points) {
        int dim = points.get(0).size();

        DenseVector meanPoint = new DenseVector(new double[dim]);
        Arrays.fill(meanPoint.values, 0);

        for (int i = 0; i < points.size(); i++) {
            for (int j = 0; j < dim; j++) {
                meanPoint.values[j] += points.get(i).values[j];
            }
        }
        for (int i = 0; i < dim; i++) {
            meanPoint.values[i] /= points.size();
        }

        return meanPoint;
    }
}
