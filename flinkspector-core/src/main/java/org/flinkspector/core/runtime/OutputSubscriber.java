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


import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class OutputSubscriber {

    /**
     * Set by the same thread that reads it.
     */
    private DataInputViewStreamWrapper inStream;

    private int instance;

    private BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>(1000);

    public byte[] recv() throws Exception {
        return getNextMessage();
    }

    public byte[] getNextMessage()  {
        try {
            return queue.poll(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            //Thread interrupted stop waiting and signal it to the subscriber
            return new byte[0];
        }
    }

    public String recvStr() throws Exception {
        return new String(getNextMessage(), StandardCharsets.UTF_8);
    }

    public void close() {

    }

    public OutputSubscriber(int instance, Disruptor<ByteEvent> disruptor) {
        this.instance = instance;
        disruptor.handleEventsWith(new ByteEventHandler());
    }

    private class ByteEventHandler implements EventHandler<ByteEvent>
    {
        public void onEvent(ByteEvent event, long sequence, boolean endOfBatch)
        {
            if(instance == event.getSender()) {
                try {
                    queue.put(event.getMsg());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
