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


import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
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

    private volatile Throwable error;

    TypeSerializer<byte[]> byteSerializer = null;
    /**
     * Set by the same thread that reads it.
     */
    private DataInputViewStreamWrapper inStream;

    Vertx vertx = Vertx.vertx();

    NetServer server;

    Buffer rest;
    int waitSize;

    private AtomicBoolean listening = new AtomicBoolean(false);

    private BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>(1000);

    public byte[] recv() throws Exception {
        return getNextMessage();
    }

    public byte[] getNextMessage() throws Exception {
        return queue.poll(1, TimeUnit.MINUTES);
    }



    public String recvStr() throws Exception {
        return new String(getNextMessage(), StandardCharsets.UTF_8);
    }

    public void close() {
        server.close();
        vertx.close();
    }

    public OutputSubscriber(ServerSocket socket) {
        throw new UnsupportedOperationException();
    }

    public OutputSubscriber(int port) {
        NetServerOptions options = new NetServerOptions().setPort(port);
        server = vertx.createNetServer(options);

        server.connectHandler(socket -> {

            socket.handler(buffer -> {
//                System.out.println("I received some bytes: " + buffer.length() + " " + buffer.toString());
                try {
                    if(rest != null) {
                        rest.appendBuffer(buffer);
                    }
                    int iend = buffer.getInt(0);
                    while (iend > 0) {
                        int l = iend + 4;
                        if(buffer.length() < l) {
                            System.out.println("rest da");
                            rest = buffer;
                            waitSize = l;
                            return;
                        }
                        Buffer slice = buffer.slice(4,l);
//                        System.out.println("s: " + slice.toString());
                        queue.put(slice.getBytes());
                        buffer = buffer.slice(l,buffer.length());
                        if(buffer.length() == 0) {
                            iend = -1;
                        } else {
                            iend = buffer.getInt(0);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            socket.closeHandler(v -> {
//                System.out.println("The socket has been closed");
            });
        });

        server.listen(port, "localhost", res -> {
            if (res.succeeded()) {
//                System.out.println("Server is now listening! " + server.actualPort());
            } else {
//                System.out.println("Failed to bind!");
            }
        });


    }


}
