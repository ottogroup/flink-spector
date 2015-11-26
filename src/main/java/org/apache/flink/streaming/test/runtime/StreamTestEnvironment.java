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

package org.apache.flink.streaming.test.runtime;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.client.JobTimeoutException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.test.functions.ParallelFromStreamRecordsFunction;
import org.apache.flink.streaming.test.functions.TestSink;
import org.apache.flink.streaming.test.input.EventTimeInput;
import org.apache.flink.streaming.test.input.Input;
import org.apache.flink.streaming.test.trigger.DefaultTestTrigger;
import org.apache.flink.streaming.test.trigger.VerifyFinishedTrigger;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamTestEnvironment extends TestStreamEnvironment {

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
	 * The current port used for transmitting the output from {@link TestSink} via 0MQ
	 * to the {@link OutputListener}s.
	 */
	private Integer currentPort;

	public StreamTestEnvironment(ForkableFlinkMiniCluster cluster, int parallelism) {
		super(cluster, parallelism);
		this.cluster = cluster;
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
	public static StreamTestEnvironment createTestEnvironment(int parallelism) throws Exception {
		ForkableFlinkMiniCluster cluster =
				TestBaseUtils.startCluster(
						1,
						parallelism,
						StreamingMode.STREAMING,
						false,
						false,
						true
				);
		return new StreamTestEnvironment(cluster, 1);
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

	/**
	 * Creates a TestSink to verify your the output of your stream.
	 * Using a {@link OutputVerifier}
	 *
	 * @param verifier {@link OutputVerifier} which will be
	 *                 used to verify the received records.
	 * @param <IN>     type of the input
	 * @return the created sink.
	 */
	public <IN> TestSink<IN> createTestSink(OutputVerifier<IN> verifier) {
		VerifyFinishedTrigger trigger = new DefaultTestTrigger();
		registerListener(currentPort, verifier, trigger);
		TestSink<IN> sink = new TestSink<IN>(currentPort);
		currentPort++;
		return sink;
	}

	/**
	 * Creates a TestSink to verify the output of your stream.
	 * The environment will register a port
	 *
	 * @param verifier which will be used to verify the received records
	 * @param <IN>     type of the input
	 * @return the created sink.
	 */
	public <IN> TestSink<IN> createTestSink(OutputVerifier<IN> verifier,
											VerifyFinishedTrigger trigger) {
		registerListener(currentPort, verifier, trigger);
		TestSink<IN> sink = new TestSink<IN>(currentPort);
		currentPort++;
		return sink;
	}

	/**
	 * Creates a new data stream that contains the given elements. The elements must all be of the same type, for
	 * example, all of the {@link String} or {@link Integer}.
	 * <p>
	 * The framework will try and determine the exact type from the elements. In case of generic elements, it may be
	 * necessary to manually supply the type information via {@link #fromCollection(java.util.Collection,
	 * org.apache.flink.api.common.typeinfo.TypeInformation)}.
	 * <p>
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism one.
	 *
	 * @param data  The array of elements to create the data stream from.
	 * @param <OUT> The type of the returned data stream
	 * @return The data stream representing the given array of elements
	 */
	@SafeVarargs
	public final <OUT> DataStreamSource<OUT> fromElementsWithTimeStamp(StreamRecord<OUT>... data) {
		if (data.length == 0) {
			throw new IllegalArgumentException("fromElements needs at least one element as argument");
		}

		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(data[0].getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + data[0].getClass().getName()
					+ "; please specify the TypeInformation manually via "
					+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}
		return fromCollectionWithTimestamp(Arrays.asList(data), typeInfo);
	}

	public <OUT> DataStreamSource<OUT> fromInput(EventTimeInput<OUT> input) {
		return fromCollectionWithTimestamp(input.getInput());
	}

	public <OUT> DataStreamSource<OUT> fromInput(Input<OUT> input) {
		return fromCollection(input.getInput());
	}

	/**
	 * Creates a data stream from the given non-empty collection. The type of the data stream is that of the
	 * elements in the collection.
	 * <p>
	 * <p>The framework will try and determine the exact type from the collection elements. In case of generic
	 * elements, it may be necessary to manually supply the type information via
	 * {@link #fromCollection(java.util.Collection, org.apache.flink.api.common.typeinfo.TypeInformation)}.</p>
	 * <p>
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * parallelism one.</p>
	 *
	 * @param data  The collection of elements to create the data stream from.
	 * @param <OUT> The generic type of the returned data stream.
	 * @return The data stream representing the given collection
	 */
	public <OUT> DataStreamSource<OUT> fromCollectionWithTimestamp(Collection<StreamRecord<OUT>> data) {
		Preconditions.checkNotNull(data, "Collection must not be null");
		if (data.isEmpty()) {
			throw new IllegalArgumentException("Collection must not be empty");
		}

		StreamRecord<OUT> first = data.iterator().next();
		if (first == null) {
			throw new IllegalArgumentException("Collection must not contain null elements");
		}

		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(first.getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + first.getClass()
					+ "; please specify the TypeInformation manually via "
					+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}
		return fromCollectionWithTimestamp(data, typeInfo);
	}

	/**
	 * Creates a data stream from the given non-empty collection.
	 * <p>
	 * <p>Note that this operation will result in a non-parallel data stream source,
	 * i.e., a data stream source with a parallelism one.</p>
	 *
	 * @param data    The collection of elements to create the data stream from
	 * @param outType The TypeInformation for the produced data stream
	 * @param <OUT>   The type of the returned data stream
	 * @return The data stream representing the given collection
	 */
	public <OUT> DataStreamSource<OUT> fromCollectionWithTimestamp(Collection<StreamRecord<OUT>> data,
																   TypeInformation<OUT> outType) {
		Preconditions.checkNotNull(data, "Collection must not be null");

		TypeInformation<StreamRecord<OUT>> typeInfo;
		StreamRecord<OUT> first = data.iterator().next();
		try {
			typeInfo = TypeExtractor.getForObject(first);
		} catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + first.getClass()
					+ "; please specify the TypeInformation manually via "
					+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}

		// must not have null elements and mixed elements
		FromElementsFunction.checkCollection(data, typeInfo.getTypeClass());

		SourceFunction<OUT> function;
		try {
			function = new ParallelFromStreamRecordsFunction<OUT>(typeInfo.createSerializer(getConfig()), data);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		return addSource(function, "Collection Source", outType).setParallelism(1);
	}

	public <OUT> DataStreamSource<OUT> fromInput(Collection<OUT> input) {
		return super.fromCollection(input);
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
				if(runningListeners.get() == 0) {
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
				if(runningListeners.get() == 0) {
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
