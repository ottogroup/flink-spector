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
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.flinkspector.core.runtime.OutputSubscriber.ResultState;
import org.flinkspector.core.trigger.VerifyFinishedTrigger;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is responsible for orchestrating tests run with Flinkspector
 */
public abstract class Runner {


    private final int random = new Random().nextInt(100);

    public void sout(Object o) {
        System.out.println(random + " -:" + o);
    }

    /**
     * {@link LocalFlinkMiniCluster} used for running the test.
     */
    private final LocalFlinkMiniCluster cluster;

    /**
     * {@link ListeningExecutorService} used for running the {@link OutputSubscriber},
     * in the background.
     */
    private final ListeningExecutorService executorService;

    ExecutorService clusterExecutor = Executors.newFixedThreadPool(4);

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

    private boolean stopped;
    private Future<?> clusterFuture;


    public Runner(LocalFlinkMiniCluster executor) {
        this.cluster = executor;
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        currentPort = 5555;
        runningListeners = new AtomicInteger(0);
        stopExecution = new TimerTask() {
            public void run() {
                sout("timer " + random);
                stopExecution();
                sout("timer out");
            }
        };


    }

    protected abstract void executeEnvironment() throws JobTimeoutException, Throwable;

    private synchronized void runLocalCluster() throws Throwable {
            try {
                executeEnvironment();
                cleanUp();
            } catch (JobTimeoutException
                    | IllegalStateException e) {
                //cluster has been shutdown forcefully, most likely by a timeout.
                failed.set(true);
                cleanUp();
            }
    }

    private void shutdownLocalCluster() throws InterruptedException {

//        ExecutorService executor = Executors.newFixedThreadPool(4);

//        Future<?> future = executor.submit((Runnable) () -> {
            try {
//                //give it a little time to finish by itself
//                Thread.sleep(1000);
                TestBaseUtils.stopCluster(cluster, new FiniteDuration(1000, TimeUnit.SECONDS));
                sout("stopCluster returned");
            } catch (Exception e) {
                e.printStackTrace();
            }
//        });

        clusterExecutor.shutdown();
//
        try {
            clusterFuture.get(6, TimeUnit.SECONDS);
            sout("stop timeout");
        } catch (ExecutionException e) {
            throw new RuntimeException("Error while shutting down the cluster: ", e);
        } catch (TimeoutException e) {
            clusterFuture.cancel(true);
        }
        // wait all unfinished tasks for 5 sec
        if (clusterExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            sout("waiting?");
            // force them to quit by interrupting
            clusterExecutor.shutdownNow();
            sout("println");
        }
        sout("shutdownLocalCluster success?");
    }


    /**
     * Stops the execution of the test.
     * <p/>
     * Shutting the local cluster down will notify
     * the sockets when the sinks are closed.
     * Thus terminating the execution gracefully.
     */
    public synchronized void stopExecution() {
        sout("Runner.stopExecution");
        stopped = true;
        //execution has failed no cleanup necessary
        if (failed.get()) {
            sout("failed");
            return;
        }
        //run is not finished and has to be stopped forcefully
        if (!finished.get()) {
            sout("cleaning");
            cleanUp();
            sout("did it");
            try {
                shutdownLocalCluster();
                sout("shut down complete");
            } catch (InterruptedException e) {
                throw new RuntimeException("Local cluster won't shutdown!");
            }
        }
        sout("stopExecutionFinished");
    }

    private synchronized void cleanUp() {
        sout("Runner.cleanUp");
        if (!finished.get()) {
            finished.set(true);
            sout("c1");
            sout("c2");
            stopTimer.cancel();
            sout("c3");
            stopTimer.purge();
            sout("c4");
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
        sout("executing...");
        stopTimer.schedule(stopExecution, timeout);
        sout("timer set");
        runLocalCluster();
        sout("execute finished");
        //====================
        // collect failures
        //====================
//        executorService.shutdown();
//        executorService.shutdownNow();
        for (ListenableFuture future : listenerFutures) {
            try {
                sout("future = " + future);
                future.get();
                sout("future?");
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
        sout("huhu");
        cleanUp();
        sout("no?");

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
    private int getAvailablePort() {
        int port = currentPort;
        currentPort++;
        return port;
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
        int port = getAvailablePort();
        ListenableFuture<OutputSubscriber.ResultState> future = executorService
                .submit(new OutputSubscriber<OUT>(port, verifier, trigger));
        runningListeners.incrementAndGet();
        listenerFutures.add(future);

        Futures.addCallback(future, new FutureCallback<ResultState>() {

            @Override
            public void onSuccess(ResultState state) {
                if (state != ResultState.SUCCESS) {
                    if (runningListeners.decrementAndGet() == 0) {
                        sout("success");
                        stopExecution();
                    }
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                //check if other sockets are still running
                if (runningListeners.decrementAndGet() == 0) {
                    sout("fail");
                    stopExecution();
                }
            }
        });

        return port;
    }
}
