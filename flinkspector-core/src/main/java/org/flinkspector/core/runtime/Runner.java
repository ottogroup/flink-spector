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
import org.apache.flink.runtime.client.JobTimeoutException;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.flinkspector.core.runtime.OutputSubscriber.ResultState;
import org.flinkspector.core.trigger.VerifyFinishedTrigger;
import org.zeromq.ZMQ;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is responsible for orchestrating tests run with Flinkspector
 */
public abstract class Runner {


    /**
     * {@link ForkableFlinkMiniCluster} used for running the test.
     */
    private final ForkableFlinkMiniCluster cluster;

    /**
     * {@link ListeningExecutorService} used for running the {@link OutputSubscriber},
     * in the background.
     */
    private final ListeningExecutorService executorService;

    /**
     * list of {@link ListenableFuture}s wrapping the {@link OutputSubscriber}s.
     */
    private final List<ListenableFuture<ResultState>> listenerFutures = new ArrayList<>();

    /**
     * Number of running sockets
     */
    private final AtomicInteger runningListeners;

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
     * to the {@link OutputSubscriber}s.
     */
    private Integer currentPort;

    private final ZMQSubscribers subscribers = new ZMQSubscribers();


    public Runner(ForkableFlinkMiniCluster executor) {
        this.cluster = executor;
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        currentPort = 5555;
        runningListeners = new AtomicInteger(0);
        stopExecution = new TimerTask() {
            public void run() {
                stopExecution();
            }
        };


    }

    private class ZMQSubscribers {

        private final ZMQ.Context context;
        private List<ZMQ.Socket> sockets = new ArrayList<>();

        ZMQSubscribers() {
            context = ZMQ.context(2);
        }

        ZMQ.Socket getSubscriber(String address) {
            ZMQ.Socket subscriber = context.socket(ZMQ.PULL);
            subscriber.setLinger(1000);

            subscriber.bind(address);

            sockets.add(subscriber);
            return subscriber;
        }

        public void close() {
            for (ZMQ.Socket s : sockets) {
                s.close();
            }
            try {
                context.term();
            } catch (IllegalStateException e) {
                //shit happens
            }
        }

    }

    protected abstract void executeEnvironment() throws JobTimeoutException, Throwable;

    private void shutdownLocalCluster() throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(4);

        Future<?> future = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //give it a little time to finish by itself
                    Thread.sleep(1000);
                    TestBaseUtils.stopCluster(cluster, new FiniteDuration(1000, TimeUnit.SECONDS));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        executor.shutdown();

        try {
            future.get(6, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            System.out.println("caught exception: " + e.getCause());
        } catch (TimeoutException e) {
            future.cancel(true);
        }
        // wait all unfinished tasks for 5 sec
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            // force them to quit by interrupting
            executor.shutdownNow();
        }
    }


    /**
     * Stop the execution of the test.
     * <p/>
     * Shutting the local cluster down will notify
     * the sockets when the sinks are closed.
     * Thus terminating the execution gracefully.
     */
    public synchronized void stopExecution() {
        //execution has failed no cleanup necessary
        if (failed.get()) {
            return;
        }
        //run is not finished and has to be stopped forcefully
        if (!finished.get()) {
            cleanUp();
            System.out.println("Killing it");
            try {
                shutdownLocalCluster();
            } catch (InterruptedException e) {
                throw new RuntimeException("Local cluster won't shutdown!");
            }
        }
    }

    private synchronized void cleanUp() {
        if (!finished.get()) {
            System.out.println("Runner.cleanUp");
            subscribers.close();
            stopTimer.cancel();
            stopTimer.purge();
            finished.set(true);
        }
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
        stopTimer.schedule(stopExecution, timeout);
        try {
            executeEnvironment();
        } catch (JobTimeoutException
                | IllegalStateException e) {
            //cluster has been shutdown forcefully, most likely by a timeout.
            failed.set(true);
            cleanUp();
        }

        //====================
        // collect failures
        //====================
        for (ListenableFuture future : listenerFutures) {
            try {
                future.get();
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
     * failed prematurely by e.g. a timeout.
     *
     * @return true if has been failed forcefully.
     */
    public Boolean hasBeenStopped() {
        return failed.get();
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
    private int getAvailablePort() {
        int port = currentPort;
        currentPort++;
        return port;
    }

    /**
     * Registers a verifier for a 0MQ port.
     *
     * @param <OUT>E
     * @param verifier verifier
     * @param trigger
     */
    public <OUT> int registerListener(OutputVerifier<OUT> verifier,
                                      VerifyFinishedTrigger<? super OUT> trigger) {
        int port = getAvailablePort();

        ZMQ.Socket subscriber = subscribers.getSubscriber("tcp://127.0.0.1:" + port);

        ListenableFuture<OutputSubscriber.ResultState> future = executorService
                .submit(new OutputSubscriber<OUT>(subscriber, verifier, trigger));
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

        return port;
    }

}
