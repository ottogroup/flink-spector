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

import org.flinkspector.core.input.Input;
import org.flinkspector.core.runtime.OutputVerifier;
import org.flinkspector.core.runtime.Runner;
import org.flinkspector.core.trigger.DefaultTestTrigger;
import org.flinkspector.core.trigger.VerifyFinishedTrigger;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.test.util.TestEnvironment;

public class DataSetTestEnvironment extends TestEnvironment{


	private final Runner runner;

	public DataSetTestEnvironment(ForkableFlinkMiniCluster executor, int parallelism) {
		super(executor, parallelism);
		runner = new Runner(executor) {
			@Override
			protected void executeEnvironment() throws Throwable {
				execute();
			}
		};
	}

	/**
	 * Factory method to create a new instance, providing a
	 * new instance of {@link ForkableFlinkMiniCluster}
	 *
	 * @param parallelism global setting for parallel execution.
	 * @return new instance of {@link DataSetTestEnvironment}
	 * @throws Exception
	 */
	public static DataSetTestEnvironment createTestEnvironment(int parallelism) throws Exception {
		ForkableFlinkMiniCluster cluster =
				TestBaseUtils.startCluster(
						1,
						parallelism,
						StreamingMode.STREAMING,
						false,
						false,
						true
				);
		return new DataSetTestEnvironment(cluster, 1);
	}

	public <T> DataSet<T> createTestSet(Input<T> input) {
		return super.fromCollection(input.getInput());
	}

	/**
	 * Creates a TestOutputFormat to verify the output.
	 * Using a {@link OutputVerifier}
	 *
	 * @param verifier {@link OutputVerifier} which will be
	 *                 used to verify the received records.
	 * @param <IN>     type of the input
	 * @return the created {@link TestOutputFormat}.
	 */
	public <IN> TestOutputFormat<IN> createTestOutputFormat(OutputVerifier<IN> verifier) {
		VerifyFinishedTrigger trigger = new DefaultTestTrigger();
		int port = runner.registerListener(verifier, trigger);
		TestOutputFormat<IN> format = new TestOutputFormat<>(port);
		return format;
	}

	/**
	 * Creates a TestOutputFormat to verify the output.
	 * The environment will register a port
	 *
	 * @param verifier which will be used to verify the received records
	 * @param <IN>     type of the input
	 * @return the created sink.
	 */
	public <IN> TestOutputFormat<IN> createTestOutputFormat(OutputVerifier<IN> verifier,
	                                                        VerifyFinishedTrigger trigger) {
		int port = runner.registerListener(verifier, trigger);
		TestOutputFormat<IN> format = new TestOutputFormat<IN>(port);
		return format;
	}

	/**
	 * This method can be used to check if the environment has been
	 * stopped prematurely by e.g. a timeout.
	 *
	 * @return true if has been stopped forcefully.
	 */
	public Boolean hasBeenStopped() {
		return runner.hasBeenStopped();
	}

	/**
	 * Getter for the timeout interval
	 * after the test execution gets stopped.
	 *
	 * @return timeout in milliseconds
	 */
	public Long getTimeoutInterval() {
		return runner.getTimeoutInterval();
	}

	/**
	 * Setter for the timeout interval
	 * after the test execution gets stopped.
	 *
	 * @param interval
	 */
	public void setTimeoutInterval(long interval) {
		runner.setTimeoutInterval(interval);
	}

	public void executeTest() throws Throwable {
		runner.executeTest();
	}

}
