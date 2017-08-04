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

package io.flinkspector.core.runtime;


import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Receives the incoming events from the sinks during the tests
 * Receiving a message will block until a message is available or a timeout triggers.
 */
public class OutputSubscriber {

    /**
     * timeout in minutes for receiving a message
     */
    private final long TIMEOUT = 1;
    /**
     * id of the sink instance this subscriber is paired with
     */
    private int instance;
    /**
     * queue for storing incoming events
     */
    private BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>(1000);

    /**
     * Constructor
     *
     * @param instance  id of paired sink
     * @param disruptor disruptor transporting the messages
     */
    public OutputSubscriber(int instance, Disruptor<OutputEvent> disruptor) {
        this.instance = instance;
        disruptor.handleEventsWith(new ByteEventHandler());
    }

    /**
     * Receives a byte array.
     * Will block until data is available!
     *
     * @return received byte array.
     */
    public byte[] recv() {
        return getNextMessage();
    }

    /**
     * Get the next message.
     * Will block until data is available!
     *
     * @return byte array with serialized message
     */
    public byte[] getNextMessage() {
        try {
            return queue.poll(TIMEOUT, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            //Thread interrupted stop waiting and signal it to the subscriber
            return new byte[0];
        }
    }

    /**
     * Receive the next message and encode into string.
     * Will block until message is available!
     * Used only for testing.
     *
     * @return string respresentation of the message
     * @throws Exception
     */
    public String recvStr() throws Exception {
        return new String(getNextMessage(), StandardCharsets.UTF_8);
    }

    /**
     * closes the subscriber
     */
    public void close() {

    }

    private class ByteEventHandler implements EventHandler<OutputEvent> {

        public void onEvent(OutputEvent event, long sequence, boolean endOfBatch) {
            if (instance == event.getSender()) {
                try {
                    queue.put(event.getMsg());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
