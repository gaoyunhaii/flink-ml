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

package org.apache.flink.iteration.broadcast;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.iteration.utils.ReflectionUtils;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Field;

/** The reflection utilities to parse the output and create the broadcast output. */
public class OutputReflectionContext {
    private final Class<?> broadcastingOutputClass;
    private final Field broadcastingOutputsField;

    private final Class<?> copyingBroadcastingOutputClass;

    private final Class<?> chainingOutputClass;
    private final Field chainingOutputTagField;

    private final Class<?> copyingChainingOutputClass;

    private final Field recordWriterOutputTagField;
    private final Field recordWriterField;
    private final Field recordWriterSerializationDelegateField;
    private final Field serializationDelegateSerializerField;

    public OutputReflectionContext() {
        try {
            this.broadcastingOutputClass =
                    Class.forName(
                            "org.apache.flink.streaming.runtime.tasks.BroadcastingOutputCollector");
            this.broadcastingOutputsField =
                    ReflectionUtils.getClassField(broadcastingOutputClass, "outputs");

            this.copyingBroadcastingOutputClass =
                    Class.forName(
                            "org.apache.flink.streaming.runtime.tasks.CopyingBroadcastingOutputCollector");

            this.chainingOutputClass =
                    Class.forName("org.apache.flink.streaming.runtime.tasks.ChainingOutput");
            this.chainingOutputTagField =
                    ReflectionUtils.getClassField(chainingOutputClass, "outputTag");

            this.copyingChainingOutputClass =
                    Class.forName("org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput");

            this.recordWriterOutputTagField =
                    ReflectionUtils.getClassField(RecordWriterOutput.class, "outputTag");
            this.recordWriterField =
                    ReflectionUtils.getClassField(RecordWriterOutput.class, "recordWriter");
            this.recordWriterSerializationDelegateField =
                    ReflectionUtils.getClassField(
                            RecordWriterOutput.class, "serializationDelegate");
            this.serializationDelegateSerializerField =
                    ReflectionUtils.getClassField(SerializationDelegate.class, "serializer");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the OutputReflectionContext", e);
        }
    }

    public boolean isBroadcastingOutput(Output<?> rawOutput) {
        return broadcastingOutputClass.isAssignableFrom(rawOutput.getClass());
    }

    public boolean isCopyingBroadcastingOutput(Output<?> rawOutput) {
        return copyingBroadcastingOutputClass.isAssignableFrom(rawOutput.getClass());
    }

    public Class<?> getBroadcastingOutputClass() {
        return broadcastingOutputClass;
    }

    public Class<?> getCopyingBroadcastingOutputClass() {
        return copyingBroadcastingOutputClass;
    }

    public boolean isChainingOutput(Output<?> rawOutput) {
        return chainingOutputClass.isAssignableFrom(rawOutput.getClass());
    }

    public boolean isCopyingChainingOutput(Output<?> rawOutput) {
        return copyingChainingOutputClass.isAssignableFrom(rawOutput.getClass());
    }

    public Class<?> getChainingOutputClass() {
        return chainingOutputClass;
    }

    public Class<?> getCopyingChainingOutputClass() {
        return copyingChainingOutputClass;
    }

    public boolean isRecordWriterOutput(Output<?> rawOutput) {
        return RecordWriterOutput.class.isAssignableFrom(rawOutput.getClass());
    }

    public <OUT> Output<StreamRecord<OUT>>[] getBroadcastingInternalOutputs(Object output) {
        return ReflectionUtils.getFieldValue(output, broadcastingOutputsField);
    }

    public OutputTag<?> getChainingOutputTag(Object output) {
        return ReflectionUtils.getFieldValue(output, chainingOutputTagField);
    }

    public OutputTag<?> getRecordWriterOutputTag(Object output) {
        return ReflectionUtils.getFieldValue(output, recordWriterOutputTagField);
    }

    @SuppressWarnings("unchecked")
    public RecordWriter<SerializationDelegate<StreamElement>> getRecordWriter(Object output) {
        return ReflectionUtils.getFieldValue(output, recordWriterField);
    }

    @SuppressWarnings("unchecked")
    public TypeSerializer<StreamElement> getRecordWriterTypeSerializer(Object output) {
        SerializationDelegate<StreamElement> serializationDelegate =
                ReflectionUtils.getFieldValue(output, recordWriterSerializationDelegateField);
        TypeSerializer<StreamElement> typeSerializer =
                ReflectionUtils.getFieldValue(
                        serializationDelegate, serializationDelegateSerializerField);
        return typeSerializer.duplicate();
    }
}
