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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.client.JobTimeoutException;
import org.apache.flink.streaming.test.input.Input;
import org.apache.flink.streaming.test.runtime.OutputListener;
import org.apache.flink.streaming.test.runtime.OutputVerifier;
import org.apache.flink.streaming.test.runtime.StreamTestFailedException;
import org.apache.flink.streaming.test.trigger.DefaultTestTrigger;
import org.apache.flink.streaming.test.trigger.VerifyFinishedTrigger;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.test.util.TestEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRunner extends TestEnvironment{

	/**
	 * {@link ForkableFlinkMiniCluster} used for running the test.
	 */
	private final ForkableFlinkMiniCluster cluster;

	/**
	 * {@link ListeningExecutorService} used for running the {@link OutputListener},
	 * in the background.
	 */
	private final ListeningExecutorService executorService;

	/**
	 * list of {@link ListenableFuture}s wrapping the {@link OutputListener}s.
	 */
	private final List<ListenableFuture<Boolean>> listeners = new ArrayList<>();

	/**
	 * Number of running listeners
	 */
	private final AtomicInteger runningListeners;

	/**
	 * Flag indicating whether the env has been shutdown forcefully.
	 */
	private final AtomicBoolean stopped = new AtomicBoolean(false);

	/**
	 * Time in milliseconds before the test gets stopped with a timeout.
	 */
	private long timeout = 3000;

	/**
	 * {@link TimerTask} to stop the test execution.
	 */
	private TimerTask stopExecution;

	/**
	 * {@link Timer} to stop the execution
	 */
	Timer stopTimer = new Timer();

	/**
	 * The current port used for transmitting the output from {@link TestOutputFormat} via 0MQ
	 * to the {@link OutputListener}s.
	 */
	private Integer currentPort;

	public TestRunner(ForkableFlinkMiniCluster executor, int parallelism) {
		super(executor, parallelism);
		this.cluster = executor;
		executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
		currentPort = 5555;
		runningListeners = new AtomicInteger(0);
		stopExecution = new TimerTask() {
			public void run() {
				System.out.println("no timer!");
				stopExecution();
			}
		};
	}

	/**
	 * Factory method to create a new instance, providing a
	 * new instance of {@link ForkableFlinkMiniCluster}
	 *
	 * @param parallelism global setting for parallel execution.
	 * @return new instance of StreamTestEnvironment
	 * @throws Exception
	 */
	public static TestRunner createTestEnvironment(int parallelism) throws Exception {
		ForkableFlinkMiniCluster cluster =
				TestBaseUtils.startCluster(
						1,
						parallelism,
						StreamingMode.STREAMING,
						false,
						false,
						true
				);
		return new TestRunner(cluster, 1);
	}

	/**
	 * Stop the execution of the test.
	 * <p>
	 * Shutting the local cluster down will, will notify
	 * the listeners when the sinks are closed.
	 * Thus terminating the execution gracefully.
	 */
	public synchronized void stopExecution() {
		if (stopped.get()) {
			return;
		}
		System.out.println("why?");
		stopped.set(true);
		stopTimer.cancel();
		stopTimer.purge();
		cluster.shutdown();
	}

	/**
	 * Starts the test execution.
	 * Collects the results from listeners after
	 * the cluster has terminated.
	 *
	 * @throws Throwable any Exception that has occurred
	 *                   during validation the test.
	 */
	public void executeTest() throws Throwable {
		stopTimer.schedule(stopExecution, timeout);
		try {
			super.execute();
		} catch (JobTimeoutException e) {
			//cluster has been shutdown forcefully, most likely by at timeout.
			stopped.set(true);
		}
		runningListeners.set(0);

		//====================
		// collect failures
		//====================
		for (ListenableFuture future : listeners) {
			try {
				future.get();
			} catch (ExecutionException e) {
				//check if it is a StreamTestFailedException
				if (e.getCause() instanceof StreamTestFailedException) {
					//unwrap exception
					throw e.getCause().getCause();
				}
				throw e.getCause();
			}
		}
		stopTimer.cancel();
		stopTimer.purge();
	}


	public <T> DataSet<T> createTestSet(Input<T> input) {
		return super.fromCollection(input.getInput());
	}

	/**
	 * Creates a TestOutputFormat to verify your the output of your stream.
	 * Using a {@link OutputVerifier}
	 *
	 * @param verifier {@link OutputVerifier} which will be
	 *                 used to verify the received records.
	 * @param <IN>     type of the input
	 * @return the created sink.
	 */
	public <IN> TestOutputFormat<IN> createTestOutputFormat(OutputVerifier<IN> verifier) {
		VerifyFinishedTrigger trigger = new DefaultTestTrigger();
		registerListener(currentPort, verifier, trigger);
		TestOutputFormat<IN> format = new TestOutputFormat<>(currentPort);
		currentPort++;
		return format;
	}

	/**
	 * Creates a TestOutputFormat to verify the output of your stream.
	 * The environment will register a port
	 *
	 * @param verifier which will be used to verify the received records
	 * @param <IN>     type of the input
	 * @return the created sink.
	 */
	public <IN> TestOutputFormat<IN> createTestOutputFormat(OutputVerifier<IN> verifier,
	                                                        VerifyFinishedTrigger trigger) {
		registerListener(currentPort, verifier, trigger);
		TestOutputFormat<IN> format = new TestOutputFormat<IN>(currentPort);
		currentPort++;
		return format;
	}

	/**
	 * This method can be used to check if the environment has been
	 * stopped prematurely by e.g. a timeout.
	 *
	 * @return true if has been stopped forcefully.
	 */
	public Boolean hasBeenStopped() {
		return stopped.get();
	}

	/**
	 * Getter for the timeout interval
	 * after the test execution gets stopped.
	 *
	 * @return timeout in milliseconds
	 */
	public Long getTimeoutInterval() {
		return timeout;
	}

	/**
	 * Setter for the timeout interval
	 * after the test execution gets stopped.
	 *
	 * @param interval
	 */
	public void setTimeoutInterval(long interval) {
		timeout = interval;
	}

	/**
	 * Registers a verifier for a 0MQ port.
	 *
	 * @param <OUT>
	 * @param port     to listen on.
	 * @param verifier verifier
	 * @param trigger
	 */
	private <OUT> void registerListener(int port,
	                                    OutputVerifier<OUT> verifier,
	                                    VerifyFinishedTrigger trigger) {
		ListenableFuture<Boolean> future = executorService
				.submit(new OutputListener<OUT>(port, verifier, trigger));
		runningListeners.incrementAndGet();
		listeners.add(future);

		Futures.addCallback(future, new FutureCallback<Boolean>() {

			@Override
			public void onSuccess(Boolean continueExecution) {
				if (runningListeners.get() == 0) {
					//execution of environment already finished
					return;
				}
				if (!continueExecution) {
					if (runningListeners.decrementAndGet() == 0) {
						stopExecution();
					}
				}
			}

			@Override
			public void onFailure(Throwable throwable) {
				if (runningListeners.get() == 0) {
					//execution of environment already finished
					return;
				}
				//check if other listeners are still running
				if (runningListeners.decrementAndGet() == 0) {
					System.out.println("no failure!");
					stopExecution();
				}
			}
		});
	}


}
