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

package org.apache.flink.iteration.minibatch;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.iteration.typeinfo.IterationRecordTypeInfo;
import org.apache.flink.iteration.typeinfo.ReusedIterationRecordSerializer;

import java.util.Objects;

/** The type info of the minibatch. */
public class MiniBatchRecordTypeInfo<T> extends TypeInformation<MiniBatchRecord<T>> {

    private final IterationRecordTypeInfo<T> iterationRecordTypeInfo;

    public MiniBatchRecordTypeInfo(IterationRecordTypeInfo<T> iterationRecordTypeInfo) {
        this.iterationRecordTypeInfo = iterationRecordTypeInfo;
    }

    public IterationRecordTypeInfo<T> getIterationRecordTypeInfo() {
        return iterationRecordTypeInfo;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<MiniBatchRecord<T>> getTypeClass() {
        return (Class) MiniBatchRecord.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<MiniBatchRecord<T>> createSerializer(ExecutionConfig executionConfig) {
        return new ReusedMiniBatchRecordSerializer<>(
                (ReusedIterationRecordSerializer<T>)
                        iterationRecordTypeInfo.createSerializer(executionConfig));
    }

    @Override
    public String toString() {
        return "Mini-batch record type";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof MiniBatchRecordTypeInfo)) {
            return false;
        }

        MiniBatchRecordTypeInfo<?> that = (MiniBatchRecordTypeInfo<?>) o;
        return Objects.equals(iterationRecordTypeInfo, that.iterationRecordTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(iterationRecordTypeInfo);
    }

    @Override
    public boolean canEqual(Object o) {
        return o instanceof MiniBatchRecordTypeInfo;
    }
}
