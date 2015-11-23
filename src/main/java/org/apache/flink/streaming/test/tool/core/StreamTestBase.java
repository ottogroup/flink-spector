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

package org.apache.flink.streaming.test.tool.core;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.test.tool.runtime.VerifyFinishedTrigger;
import org.apache.flink.streaming.test.tool.runtime.input.EventTimeInput;
import org.apache.flink.streaming.test.tool.runtime.input.Input;
import org.apache.flink.streaming.test.tool.runtime.output.OutputVerifier;
import org.apache.flink.streaming.test.tool.runtime.output.TestSink;
import org.apache.flink.streaming.test.tool.core.assertion.HamcrestVerifier;
import org.apache.flink.streaming.test.tool.core.assertion.OutputMatcher;
import org.apache.flink.streaming.test.tool.runtime.StreamTestEnvironment;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;

/**
 * Offers a base for testing Flink streaming applications
 * To use, extend your JUnitTest with this class.
 */
public class StreamTestBase {

	/**
	 * Test Environment
	 */
	private StreamTestEnvironment env;

	/**
	 * Creates a new {@link StreamTestEnvironment}
	 */
	@Before
	public void initialize() throws Exception {
		env = StreamTestEnvironment.createTestEnvironment(2);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	}

	/**
	 * Creates a DataStreamSource from an EventTimeInput object.
	 * The DataStreamSource will emit the records with the specified EventTime.
	 *
	 * @param input to emit
	 * @param <OUT> type of the emitted records
	 * @return a DataStreamSource generating the input
	 */
	public <OUT> DataStreamSource<OUT> createTestStream(EventTimeInput<OUT> input) {
		return env.fromInput(input);
	}

	/**
	 * Creates a DataStreamSource from an EventTimeInput object.
	 * The DataStreamSource will emit the records with the specified EventTime.
	 *
	 * @param input to emit
	 * @param <OUT> type of the emitted records
	 * @return a DataStreamSource generating the input
	 *
	 */
	public <OUT> DataStreamSource<OUT> createTestStream(Collection<OUT> input) {
		return env.fromInput(input);
	}

	/**
	 * Creates a DataStreamSource from an Input object.
	 *
	 * @param input to emit
	 * @param <OUT> type of the emitted records
	 * @return a DataStream generating the input
	 */
	public <OUT> DataStreamSource<OUT> createTestStream(Input<OUT> input) {
		return env.fromInput(input);
	}

	/**
	 * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
	 *
	 * @param matcher of type Iterable<IN>
	 * @param <IN>
	 * @return the created sink.
	 */
	public <IN> TestSink<IN> createTestSink(org.hamcrest.Matcher<Iterable<IN>> matcher) {
		OutputVerifier<IN> verifier = new HamcrestVerifier<IN>(matcher);
		return env.createTestSink(verifier);
	}

	/**
	 * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
	 *
	 * @param matcher of type Iterable<IN>
	 * @param <IN>
	 * @return the created sink.
	 */
	public <IN> TestSink<IN> createTestSink(org.hamcrest.Matcher<Iterable<IN>> matcher,
											VerifyFinishedTrigger trigger) {
		OutputVerifier<IN> verifier = new HamcrestVerifier<>(matcher);
		return env.createTestSink(verifier,trigger);
	}

	/**
	 * Inspect a {@link DataStream} using a {@link OutputMatcher}.
	 *
	 * @param stream  {@link DataStream} to test.
	 * @param matcher {@link OutputMatcher} to use.
	 * @param <T>     type of the stream.
	 */
	public <T> void assertStream(DataStream<T> stream, OutputMatcher<T> matcher) {
		stream.addSink(createTestSink(matcher));
	}

	/**
	 * Inspect a {@link DataStream} using a {@link OutputMatcher}.
	 *
	 * @param stream  {@link DataStream} to test.
	 * @param matcher {@link OutputMatcher} to use.
	 * @param <T>     type of the stream.
	 */
	public <T> void assertStream(DataStream<T> stream, Matcher<Iterable<T>> matcher) {
		stream.addSink(createTestSink(matcher));
	}

	/**
	 * Inspect a {@link DataStream} using a {@link OutputMatcher}.
	 *
	 * @param stream  {@link DataStream} to test.
	 * @param matcher {@link OutputMatcher} to use.
	 * @param trigger {@link VerifyFinishedTrigger}
	 *                to finish the assertion early.
	 * @param <T>     type of the stream.
	 */
	public <T> void assertStream(DataStream<T> stream,
								 OutputMatcher<T> matcher,
								 VerifyFinishedTrigger trigger) {
		stream.addSink(createTestSink(matcher,trigger));
	}

	public void setParallelism(int n) {
		env.setParallelism(n);
	}

	/**
	 * Executes the test and verifies the output received by the sinks.
	 */
	@After
	public void executeTest() throws Throwable {
		try {
			env.executeTest();
		} catch (AssertionError assertionError) {
			if (env.hasBeenStopped()) {
				//the execution has been forcefully stopped inform the user!
				throw new AssertionError("Test terminated due timeout!" +
						assertionError.getMessage());
			}
			throw assertionError;
		}
	}

}
