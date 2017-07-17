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

package org.flinkspector.core.runtime;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.flink.runtime.client.JobTimeoutException;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.flinkspector.core.runtime.OutputHandler.ResultState;
import org.flinkspector.core.trigger.VerifyFinishedTrigger;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mortbay.util.IO.bufferSize;

/**
 * This class is responsible for orchestrating tests run with Flinkspector
 */
public abstract class Runner {


    Executor executor = Executors.newCachedThreadPool();

    ByteEventFactory factory = new ByteEventFactory();

    Disruptor<ByteEvent> disruptor = new Disruptor<>(factory, bufferSize, executor);

    ResultState state = null;

    /**
     * {@link LocalFlinkMiniCluster} used for running the test.
     */
    private final LocalFlinkMiniCluster cluster;

    private final List<ServerSocket> sockets = new ArrayList<ServerSocket>();

    /**
     * {@link ListeningExecutorService} used for running the {@link OutputHandler},
     * in the background.
     */
    private final ListeningExecutorService executorService;

    /**
     * list of {@link ListenableFuture}s wrapping the {@link OutputHandler}s.
     */
    private final List<ListenableFuture<ResultState>> listenerFutures = new ArrayList<>();

    /**
     * Number of running sockets
     */
    private final AtomicInteger runningListeners;
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Flag indicating whether the previous test has been finished .
     */
    private final AtomicBoolean finished = new AtomicBoolean(false);

    /**
     * Flag indicating whether the env has been shutdown forcefully.
     */
    private final AtomicBoolean failed = new AtomicBoolean(false);

    /**
     * Time in milliseconds before the test gets failed with a timeout.
     */
    private long timeout = 4000;

    /**
     * {@link TimerTask} to stop the test execution.
     */
    private TimerTask stopExecution;

    /**
     * {@link Timer} to stop the execution
     */
    Timer stopTimer = new Timer();

    /**
     * The current port used for transmitting the output from via 0MQ
     * to the {@link OutputHandler}s.
     */
    private Integer currentInstance;

    private boolean stopped;

    public Runner(LocalFlinkMiniCluster executor) {
        this.cluster = executor;
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        currentInstance = 1;
        runningListeners = new AtomicInteger(0);
        stopExecution = new TimerTask() {
            public void run() {
                stopExecution();
            }
        };
    }

    protected abstract void executeEnvironment() throws JobTimeoutException, Throwable;

    private synchronized void runLocalCluster() throws Throwable {
        try {
            running.set(true);
            executeEnvironment();
            finished.set(true);
            cleanUp();
        } catch (JobTimeoutException
                | IllegalStateException e) {
            //cluster has been shutdown forcefully, most likely by a timeout.
            cleanUp();
            failed.set(true);
        }
    }

    private void shutdownLocalCluster() throws InterruptedException {
        try {
            TestBaseUtils.stopCluster(cluster, new FiniteDuration(1000, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Stops the execution of the test.
     * <p/>
     * Shutting the local cluster down will notify
     * the sockets when the sinks are closed.
     * Thus terminating the execution gracefully.
     */
    public synchronized void stopExecution() {
        System.out.println("++++++++++   stopping schedoscope");
        stopped = true;
        //execution has failed no cleanup necessary
        if (failed.get()) {
            return;
        }

        if (!finished.get()) {
            //run is not finished and has to be stopped forcefully
            cleanUp();
            try {
                shutdownLocalCluster();
            } catch (InterruptedException e) {
                throw new RuntimeException("Local cluster won't shutdown!");
            }
        }
    }

    private void cancelListener() {
        for (ListenableFuture<ResultState> f: listenerFutures) {
            f.cancel(true);
        }
    }

    private synchronized void cleanUp() {
        if (!finished.get()) {
            disruptor.shutdown();
            cancelListener();
            finished.set(true);
        }
        stopTimer.cancel();
        stopTimer.purge();
    }


    /**
     * Starts the test execution.
     * Collects the results from sockets after
     * the cluster has terminated.
     *
     * @throws Throwable any Exception that has occurred
     *                   during validation the test.
     */
    public void executeTest() throws Throwable {
        disruptor.start();
        stopTimer.schedule(stopExecution, timeout);
        runLocalCluster();
        //====================
        // collect failures
        //====================
        for (ListenableFuture future : listenerFutures) {
            try {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                //timeout for future expired
                cleanUp();
                cancelListener();
                return;
            } catch (ExecutionException e) {
                //check if it is a FlinkTestFailedException
                if (e.getCause() instanceof FlinkTestFailedException) {
                    //unwrap exception
                    throw e.getCause().getCause();
                }
                if (!failed.get()) {
                    throw e.getCause();
                }
            }
        }
        cleanUp();
    }

    /**
     * This method can be used to check if the environment has been
     * stopped prematurely by e.g. a timeout.
     *
     * @return true if has been failed forcefully.
     */
    public Boolean hasBeenStopped() {
        return stopped;
    }

    /**
     * Getter for the timeout interval
     * after the test execution gets failed.
     *
     * @return timeout in milliseconds
     */
    public Long getTimeoutInterval() {
        return timeout;
    }

    /**
     * Setter for the timeout interval
     * after the test execution gets failed.
     *
     * @param interval
     */
    public void setTimeoutInterval(long interval) {
        timeout = interval;
    }

    /**
     * Get a port to use.
     *
     * @return unused port.
     */
    private int getNextInstance() {
        int instance = currentInstance;
        currentInstance++;
        return instance;
    }


    public RingBuffer<ByteEvent> getRingBuffer() {
        return disruptor.getRingBuffer();
    }

    /**
     * Registers a verifier for a 0MQ port.
     *
     * @param <OUT>
     * @param verifier verifier
     * @param trigger
     */
    public <OUT> int registerListener(OutputVerifier<OUT> verifier,
                                      VerifyFinishedTrigger<? super OUT> trigger) {

        int instance = getNextInstance();

        ListenableFuture<OutputHandler.ResultState> future = executorService
                .submit(new OutputHandler<OUT>(instance, disruptor, verifier, trigger));
        runningListeners.incrementAndGet();
        listenerFutures.add(future);

        Futures.addCallback(future, new FutureCallback<ResultState>() {

            @Override
            public void onSuccess(ResultState state) {
                if (state != ResultState.SUCCESS) {
                    if (runningListeners.decrementAndGet() == 0) {
                        stopExecution();
                    }
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                //check if other sockets are still running
                if (runningListeners.decrementAndGet() == 0) {
                    stopExecution();
                }
            }
        });

        return instance;
    }
}
