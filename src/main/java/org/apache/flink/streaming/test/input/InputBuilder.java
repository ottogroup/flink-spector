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

package org.apache.flink.streaming.test.input;

import org.apache.flink.streaming.test.functions.ParallelFromStreamRecordsFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Builder to define Input for a
 * {@link ParallelFromStreamRecordsFunction}
 *
 * @param <T> data type
 */
public class InputBuilder<T> implements Input<T> {

	/**
	 * List of input
	 */
	List<T> input = new ArrayList<T>();

	public static <T> InputBuilder<T> create(T record) {
		InputBuilder<T> builder = new InputBuilder<T>();
		builder.emit(record);
		return builder;
	}

	/**
	 * Adds a new element to the input
	 *
	 * @param record
	 */
	public InputBuilder<T> emit(T record) {
		if(record == null) {
			throw new IllegalArgumentException("Record has too be not null!");
		}
		input.add(record);
		return this;
	}

	/**
	 * Adds a new element to the input multiple times
	 *
	 * @param record
	 * @param times to repeat
	 */
	public InputBuilder<T> emit(T record, int times) {
		if (times < 1) {
			throw new IllegalArgumentException("Times has to be greater than 1.");
		}
		for (int i = 0; i < times; i++) {
			emit(record);
		}
		return this;
	}

	/**
	 * Repeat the current input list
	 *
	 * @param times number of times the input ist will be repeated
	 */
	public InputBuilder<T> repeatAll(int times) {
		List<T> toAppend = new ArrayList<>();
		for (int i = 0; i < times; i++) {
			toAppend.addAll(input);
		}
		input.addAll(toAppend);
		return this;
	}

	/**
	 * Adds a collection of elements to the input
	 *
	 * @param records times to repeat
	 */
	public InputBuilder<T> emitAll(Collection<T> records) {
		input.addAll(records);
		return this;
	}

	@Override
	public List<T> getInput() {
		return input;
	}
}
