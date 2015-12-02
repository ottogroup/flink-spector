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

package org.flinkspector.dataset;

import org.apache.flink.api.java.DataSet;
import org.flinkspector.core.input.Input;
import org.flinkspector.core.runtime.OutputVerifier;
import org.flinkspector.core.set.MatcherBuilder;
import org.flinkspector.core.table.HamcrestVerifier;
import org.flinkspector.core.table.OutputMatcherFactory;
import org.flinkspector.core.trigger.VerifyFinishedTrigger;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;

/**
 * Offers a base to test Apache Flink dataset application.
 */
public class TestBase {

	/**
	 * Test Environment
	 */
	private DataSetTestEnvironment env;

	/**
	 * Creates a new {@link DataSetTestEnvironment}
	 */
	@Before
	public void initialize() throws Exception {
		env = DataSetTestEnvironment.createTestEnvironment(2);
	}

	/**
	 * Creates a DataSet from a {@link Collection}.
	 *
	 * @param input to emit
	 * @param <OUT> generic type of the returned data set
	 * @return a DataSet containing the input
	 *
	 */
	public <OUT> DataSet<OUT> createTestDataSet(Collection<OUT> input) {
		return env.fromCollection(input);
	}

	/**
	 * Creates a DataSet from an Input object.
	 *
	 * @param input to emit
	 * @param <OUT> generic type of the returned data set
	 * @return a DataSet containing the input
	 */
	public <OUT> DataSet<OUT> createTestDataSet(Input<OUT> input) {
		return env.fromCollection(input.getInput());
	}

	/**
	 * Creates a {@link TestOutputFormat} using {@link org.hamcrest.Matcher} to verify the output.
	 *
	 * @param matcher of type Iterable<IN>
	 * @param <IN>
	 * @return the created sink.
	 */
	public <IN> TestOutputFormat<IN> createTestOutputFormat(org.hamcrest.Matcher<Iterable<IN>> matcher) {
		OutputVerifier<IN> verifier = new HamcrestVerifier<IN>(matcher);
		return env.createTestOutputFormat(verifier);
	}

	/**
	 * Creates a {@link TestOutputFormat} using {@link org.hamcrest.Matcher} to verify the output.
	 *
	 * @param matcher of type Iterable<IN>
	 * @param <IN> generic type of the returned {@link TestOutputFormat}
	 * @return the created data set from the given.
	 */
	public <IN> TestOutputFormat<IN> createTestOutputFormat(org.hamcrest.Matcher<Iterable<IN>> matcher,
	                                        VerifyFinishedTrigger trigger) {
		OutputVerifier<IN> verifier = new HamcrestVerifier<>(matcher);
		return env.createTestOutputFormat(verifier,trigger);
	}

	/**
	 * Inspect a {@link DataSet} using a {@link OutputMatcherFactory}.
	 *
	 * @param dataSet  {@link DataSet} to test.
	 * @param matcher {@link OutputMatcherFactory} to use.
	 * @param <T>     type of the dataSet.
	 */
	public <T> void assertDataSet(DataSet<T> dataSet, Matcher<Iterable<T>> matcher) {
		dataSet.output(createTestOutputFormat(matcher));
	}

	/**
	 * Inspect a {@link DataSet} using a {@link OutputMatcherFactory}.
	 *
	 * @param dataSet  {@link DataSet} to test.
	 * @param matcher {@link OutputMatcherFactory} to use.
	 * @param trigger {@link VerifyFinishedTrigger}
	 *                to finish the assertion early.
	 * @param <T>     type of the dataSet.
	 */
	public <T> void assertDataSet(DataSet<T> dataSet,
	                             Matcher<Iterable<T>> matcher,
	                             VerifyFinishedTrigger trigger) {
		dataSet.output(createTestOutputFormat(matcher, trigger));
	}

	public void setParallelism(int n) {
		env.setParallelism(n);
	}


	/**
	 * Provides an {@link DataSetBuilder} to create an {@link DataSet}
	 *
	 * @param record the first record to emit.
	 * @param <OUT>  generic type of the returned stream.
	 * @return {@link DataSetBuilder} to work with.
	 */
	public <OUT> DataSetBuilder<OUT> createTestDataSetWith(OUT record) {
		return DataSetBuilder.createBuilder(record, env);
	}


	/**
	 * Executes the test and verifies the output received.
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

	//================================================================================
	// Syntactic sugar stuff
	//================================================================================

	public static int times(int n) {
		return n;
	}

	public static final MatcherBuilder.Order strict = MatcherBuilder.Order.STRICT;
	public static final MatcherBuilder.Order notStrict = MatcherBuilder.Order.NONSTRICT;
	public static final String ignore = null;
}
