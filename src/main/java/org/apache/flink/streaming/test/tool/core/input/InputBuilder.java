/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.core.input;

import org.apache.flink.streaming.test.tool.runtime.input.FromStreamRecordsFunction;
import org.apache.flink.streaming.test.tool.runtime.input.Input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Builder to define Input for a {@link FromStreamRecordsFunction}
 * @param <T> data type
 */
public class InputBuilder<T> implements Input<T> {

	/** List of input */
	List<T> input = new ArrayList<T>();

	public static <T> InputBuilder create(T elem) {
		InputBuilder<T> builder =  new InputBuilder<T>();
		builder.emit(elem);
		return builder;
	}

	/**
	 * Adds a new element to the input
	 * @param elem
	 */
	public void emit(T elem){
		input.add(elem);
	}

	/**
	 * Adds a collection of elements to the input
	 * @param elems
	 */
	public void addAll(Collection<T> elems) {
		input.addAll(elems);
	}

	@Override
	public List<T> getInput() {
		return input;
	}
}
