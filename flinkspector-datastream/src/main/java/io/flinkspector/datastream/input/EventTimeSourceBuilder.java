/*
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.flinkspector.datastream.input;

import io.flinkspector.datastream.DataStreamTestEnvironment;
import io.flinkspector.datastream.input.time.Moment;
import io.flinkspector.datastream.input.time.TimeSpan;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * This builder is used to define input in a fluent way.
 * The functionality of {@link EventTimeInputBuilder} is used to build a list
 * of input records. And converted to a {@link DataStreamSource}.
 *
 * @param <T>
 */
public class EventTimeSourceBuilder<T> {

    private final EventTimeInputBuilder<T> builder;
    private final DataStreamTestEnvironment env;

    public EventTimeSourceBuilder(DataStreamTestEnvironment env, T record) {
        this.env = env;
        this.builder = EventTimeInputBuilder.startWith(record);
    }

    public EventTimeSourceBuilder(DataStreamTestEnvironment env, T record, Long timestamp) {
        this.env = env;
        this.builder = EventTimeInputBuilder.startWith(record, timestamp);
    }

    public EventTimeSourceBuilder(DataStreamTestEnvironment env, T record, Moment moment) {
        this.env = env;
        this.builder = EventTimeInputBuilder.startWith(record, moment);
    }

    /**
     * Factory method used to dynamically type the {@link EventTimeSourceBuilder}
     * using the type of the provided input object.
     *
     * @param record first record to emit.
     * @param env    to work on.
     * @param <T> generic type of output
     * @return created {@link SourceBuilder}
     */
    public static <T> EventTimeSourceBuilder<T> createBuilder(T record,
                                                              DataStreamTestEnvironment env) {
        return new EventTimeSourceBuilder<>(env, record);
    }

    /**
     * Factory method used to dynamically type the {@link EventTimeSourceBuilder}
     * using the type of the provided input object.
     *
     * @param record first record to emit.
     * @param env    to work on.
     * @param <T> generic type of output
     * @return created {@link SourceBuilder}
     */
    public static <T> EventTimeSourceBuilder<T> createBuilder(T record,
                                                              Long timestamp,
                                                              DataStreamTestEnvironment env) {
        return new EventTimeSourceBuilder<>(env, record, timestamp);
    }

    /**
     * Factory method used to dynamically type the {@link EventTimeSourceBuilder}
     * using the type of the provided input object.
     *
     * @param record first record to emit.
     * @param env    to work on.
     * @param <T> generic type of output
     * @return created {@link SourceBuilder}
     */
    public static <T> EventTimeSourceBuilder<T> createBuilder(T record,
                                                              Moment moment,
                                                              DataStreamTestEnvironment env) {
        return new EventTimeSourceBuilder<>(env, record, moment);
    }

    /**
     * Produces a {@link DataStreamSource} with the predefined input.
     *
     * @return {@link DataStreamSource}
     */
    public DataStreamSource<T> close() {
        return env.fromInput(builder);
    }

    /**
     * Produces a {@link DataStreamSource} with the predefined input
     * and flushes open windows on termination.
     *
     * @return {@link DataStreamSource}
     */
    public DataStreamSource<T> closeAndFlush() {
        builder.flushOpenWindowsOnTermination();
        return close();
    }

    /**
     * Add an element with timestamp to the input.
     *
     * @param elem
     * @param timeStamp
     * @return this (used for fluent interface)
     */
    public EventTimeSourceBuilder<T> emit(T elem, long timeStamp) {
        builder.emit(new StreamRecord(elem, timeStamp));
        return this;
    }

    /**
     * Add an element with an {@link TimeSpan} object,
     * defining the time between the previous and the new record.
     *
     * @param elem
     * @param timeSpan {@link TimeSpan}
     * @return this (used for fluent interface)
     */
    public EventTimeSourceBuilder<T> emit(T elem, Moment timeSpan) {
        builder.emit(elem, timeSpan);
        return this;
    }

    /**
     * Add an element with object,
     * defining the time between the previous and the new record.
     *
     * @param elem
     * @return this (used for fluent interface)
     */
    public EventTimeSourceBuilder<T> emit(T elem) {
        builder.emit(elem);
        return this;
    }

    /**
     * Add a {@link StreamRecord} to the list of input.
     *
     * @param streamRecord
     * @return this (used for fluent interface)
     */
    public EventTimeSourceBuilder<T> emit(StreamRecord<T> streamRecord) {
        builder.emit(streamRecord);
        return this;
    }

    /**
     * Repeats the record.
     *
     * @param times number of times the input ist will be repeated.
     */
    public EventTimeSourceBuilder<T> emit(T elem, Moment timeInterval, int times) {
        builder.emit(elem, timeInterval, times);
        return this;
    }

    /**
     * Repeat the current input list, after the defined span.
     * The time span between records in your already defined list will
     * be kept.
     *
     * @param timeSpan defining the time before and between repeating.
     * @param times    number of times the input ist will be repeated.
     */
    public EventTimeSourceBuilder<T> repeatAll(TimeSpan timeSpan, int times) {
        builder.repeatAll(timeSpan, times);
        return this;
    }


}
