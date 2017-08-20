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

package io.flinkspector.dataset;

import io.flinkspector.core.Order;
import io.flinkspector.core.input.Input;
import io.flinkspector.core.quantify.HamcrestVerifier;
import io.flinkspector.core.quantify.OutputMatcherFactory;
import io.flinkspector.core.runtime.OutputVerifier;
import io.flinkspector.core.trigger.VerifyFinishedTrigger;
import org.apache.flink.api.java.DataSet;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;

/**
 * Offers a base to test Apache Flink dataset application.
 */
public class DataSetTestBase {

    public static final Order strict = Order.STRICT;
    public static final Order notStrict = Order.NONSTRICT;
    public static final String ignore = null;
    /**
     * Test Environment
     */
    public DataSetTestEnvironment testEnv;

    public static int times(int n) {
        return n;
    }

    /**
     * Creates a new {@link DataSetTestEnvironment}
     */
    @Before
    public void initialize() throws Exception {
        testEnv = DataSetTestEnvironment.createTestEnvironment(1);
    }

    /**
     * Creates a DataSet from a {@link Collection}.
     *
     * @param input to emit
     * @param <OUT> generic type of the returned data set
     * @return a DataSet containing the input
     */
    public <OUT> DataSet<OUT> createTestDataSet(Collection<OUT> input) {
        return testEnv.fromCollection(input);
    }

    /**
     * Creates a DataSet from an Input object.
     *
     * @param input to emit
     * @param <OUT> generic type of the returned data set
     * @return a DataSet containing the input
     */
    public <OUT> DataSet<OUT> createTestDataSet(Input<OUT> input) {
        return testEnv.fromCollection(input.getInput());
    }

    /**
     * Creates a {@link TestOutputFormat} using {@link org.hamcrest.Matcher} to verify the output.
     *
     * @param matcher of type Iterable
     * @param <IN>
     * @return the created sink.
     */
    public <IN> TestOutputFormat<IN> createTestOutputFormat(org.hamcrest.Matcher<Iterable<IN>> matcher) {
        OutputVerifier<IN> verifier = new HamcrestVerifier<IN>(matcher);
        return createTestOutputFormat(verifier);
    }

    /**
     * Creates a {@link TestOutputFormat} using {@link org.hamcrest.Matcher} to verify the output.
     *
     * @param matcher of type Iterable
     * @param <IN>    generic type of the returned {@link TestOutputFormat}
     * @return the created data set from the given.
     */
    public <IN> TestOutputFormat<IN> createTestOutputFormat(org.hamcrest.Matcher<Iterable<IN>> matcher,
                                                            VerifyFinishedTrigger trigger) {
        OutputVerifier<IN> verifier = new HamcrestVerifier<>(matcher);
        return createTestOutputFormat(verifier, trigger);
    }

    /**
     * Creates a {@link TestOutputFormat} using {@link org.hamcrest.Matcher} to verify the output.
     *
     * @param verifier {@link OutputVerifier}
     * @param <IN>     generic type of the returned {@link TestOutputFormat}
     * @return the created data set from the given.
     */
    public <IN> TestOutputFormat<IN> createTestOutputFormat(OutputVerifier<IN> verifier,
                                                            VerifyFinishedTrigger trigger) {
        return testEnv.createTestOutputFormat(verifier, trigger);
    }

    /**
     * Creates a {@link TestOutputFormat} using {@link org.hamcrest.Matcher} to verify the output.
     *
     * @param verifier {@link OutputVerifier}
     * @param <IN>     generic type of the returned {@link TestOutputFormat}
     * @return the created data set from the given.
     */
    public <IN> TestOutputFormat<IN> createTestOutputFormat(OutputVerifier<IN> verifier) {
        return testEnv.createTestOutputFormat(verifier);
    }

    /**
     * Inspect a {@link DataSet} using a {@link OutputMatcherFactory}.
     *
     * @param dataSet {@link DataSet} to test.
     * @param matcher {@link OutputMatcherFactory} to use.
     * @param <T>     type of the dataSet.
     */

    public <T> void assertDataSet(DataSet<T> dataSet, Matcher<Iterable<T>> matcher) {
        dataSet.output(createTestOutputFormat(matcher));
    }

    /**
     * Inspect a {@link DataSet} using a {@link OutputMatcherFactory}.
     *
     * @param dataSet {@link DataSet} to test.
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

    //================================================================================
    // Syntactic sugar stuff
    //================================================================================

    /**
     * Sets the parallelism for operations executed through this environment.
     * Setting a parallelism of x here will cause all operators (such as map,
     * batchReduce) to run with x parallel instances. This method overrides the
     * default parallelism for this environment. The
     * {@link org.apache.flink.api.java.LocalEnvironment} uses by default a value equal to the
     * number of hardware contexts (CPU cores / threads). When executing the
     * program via the command line client from a JAR file, the default degree
     * of parallelism is the one configured for that setup.
     *
     * @param parallelism The parallelism
     */
    public void setParallelism(int parallelism) {
        testEnv.setParallelism(parallelism);
    }

    /**
     * Provides an {@link DataSetBuilder} to startWith an {@link DataSet}
     *
     * @param record the first record to emit.
     * @param <OUT>  generic type of the returned stream.
     * @return {@link DataSetBuilder} to work with.
     */
    public <OUT> DataSetBuilder<OUT> createTestDataSetWith(OUT record) {
        return DataSetBuilder.createBuilder(record, testEnv);
    }

    /**
     * Executes the test and verifies the output received.
     */
    @After
    public void executeTest() throws Throwable {
        try {
            testEnv.executeTest();
        } catch (AssertionError assertionError) {
            throw assertionError;
        }
    }

    /**
     * Setter for the timeout interval
     * after the test execution gets stopped.
     *
     * @param interval in milliseconds.
     */
    public void setTimeout(long interval) {
        testEnv.setTimeoutInterval(interval);
    }
}
