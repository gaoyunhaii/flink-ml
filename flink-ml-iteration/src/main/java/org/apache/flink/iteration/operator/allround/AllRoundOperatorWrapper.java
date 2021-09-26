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

package org.apache.flink.iteration.operator.allround;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.operator.OperatorWrapper;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

/** The operator wrapper implementation for all-round wrappers. */
public class AllRoundOperatorWrapper<T> implements OperatorWrapper<T, IterationRecord<T>> {
    @Override
    public StreamOperator<IterationRecord<T>> wrap(
            StreamOperatorParameters<IterationRecord<T>> operatorParameters,
            StreamOperatorFactory<T> operatorFactory) {
        Class<? extends StreamOperator> operatorClass =
                operatorFactory.getStreamOperatorClass(getClass().getClassLoader());
        if (OneInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return new OneInputAllRoundWrapperOperator<>(operatorParameters, operatorFactory);
        } else if (TwoInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return new TwoInputAllRoundWrapperOperator<>(operatorParameters, operatorFactory);
        } else if (MultipleInputStreamOperator.class.isAssignableFrom(operatorClass)) {
            return new MultipleInputAllRoundWrapperOperator<>(operatorParameters, operatorFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported operator class for all-round wrapper: " + operatorClass);
        }
    }

    @Override
    public TypeInformation<IterationRecord<T>> getWrappedTypeInfo(TypeInformation<T> typeInfo) {
        return null;
    }
}
