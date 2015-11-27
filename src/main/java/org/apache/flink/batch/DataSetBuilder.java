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

package org.apache.flink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.core.input.InputBuilder;

import java.util.Collection;

/**
 * This builder is used to define input in a fluent way.
 * The functionality of {@link InputBuilder} is used to build a list
 * of input records. And converted to a {@link DataSet}.
 * @param <T>
 */
public class DataSetBuilder<T> {

	private final InputBuilder<T> builder = new InputBuilder<>();
	private final TestRunner env;

	public DataSetBuilder(TestRunner env) {
		this.env = env;
	}

	/**
	 * Factory method used to dynamically type the {@link DataSetBuilder}
	 * using the type of the provided input object.
	 * @param record first record to emit
	 * @param env to work on.
	 * @param <T>
	 * @return created {@link DataSetBuilder}
	 */
	public static <T> DataSetBuilder<T> createBuilder(T record,
	                                                      TestRunner env) {
		DataSetBuilder<T> sourceBuilder = new DataSetBuilder<>(env);
		return sourceBuilder.emit(record);
	}

	/**
	 * Produces a {@link DataStreamSource} with the predefined input.
	 * @return {@link DataStreamSource}
	 */
	public DataSet<T> finish() {
		return env.fromCollection(builder.getInput());
	}

	/**
	 * Adds a new element to the input
	 *
	 * @param record
	 */
	public DataSetBuilder<T> emit(T record) {
		builder.emit(record);
		return this;
	}


	/**
	 * Repeat the current input list
	 *
	 * @param times number of times the input list will be repeated
	 */
	public DataSetBuilder<T> repeatAll(int times) {
		builder.repeatAll(times);
		return this;
	}

	/**
	 * Adds a new element to the input multiple times
	 *
	 * @param record
	 * @param times
	 */
	public DataSetBuilder<T> emit(T record, int times) {
		builder.emit(record, times);
		return this;
	}

	/**
	 * Adds a collection of elements to the input
	 *
	 * @param records
	 */
	public DataSetBuilder<T> emitAll(Collection<T> records) {
		builder.emitAll(records);
		return this;
	}
}
