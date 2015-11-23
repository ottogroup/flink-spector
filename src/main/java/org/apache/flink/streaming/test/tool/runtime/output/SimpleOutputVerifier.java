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

package org.apache.flink.streaming.test.tool.runtime.output;

import org.apache.flink.streaming.test.tool.runtime.StreamTestFailedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Extend this abstract class a simple {@link OutputVerifier}
 * the verify method will be called after the final
 * record has arrived in the sink
 * @param <T>
 */
public abstract class SimpleOutputVerifier<T> implements OutputVerifier<T> {

	private List<T> output = new ArrayList<>();

	/**
	 * This method is called once all output has arrived in the {@link TestSink}
	 * to verify the output.
	 * @param output from the test run
	 */
	abstract public void verify(List<T> output) throws StreamTestFailedException;

	@Override
	public void init() {
		output = new ArrayList<>();
	}

	@Override
	public void receive(T record) throws StreamTestFailedException {
		output.add(record);
	}

	@Override
	public void finish() throws StreamTestFailedException {
		verify(output);
	}

}
