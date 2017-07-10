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


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
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

    /**
     * The socket for the specific stream.
     */
    private List<StreamHandler> handlers = new ArrayList<StreamHandler>();

    private final ServerSocket socket;

    private AtomicBoolean listening = new AtomicBoolean(false);

    private BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>(1000);

    public byte[] recv() throws Exception {
        return readNextFromStream();
    }

    public void listenForConnection() {
        if (listening.compareAndSet(false, true))
            new ConnectionListener().start();
    }

    public byte[] readNextFromStream() throws Exception {
        return queue.poll(1, TimeUnit.MINUTES);
    }

    public String recvStr() throws Exception {
        return new String(readNextFromStream(), StandardCharsets.UTF_8);
    }

    public OutputSubscriber(ServerSocket socket) {
        ExecutionConfig config = new ExecutionConfig();
        TypeInformation<byte[]> typeInfo = TypeExtractor.getForObject(new byte[0]);
        byteSerializer = typeInfo.createSerializer(config);
        this.socket = socket;
        listenForConnection();
    }

    public OutputSubscriber(int port) {

        ExecutionConfig config = new ExecutionConfig();
        TypeInformation<byte[]> typeInfo = TypeExtractor.getForObject(new byte[0]);
        byteSerializer = typeInfo.createSerializer(config);

        try {
            socket = new ServerSocket(port, 1);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not open socket to receive back stream results");
        }
        listenForConnection();
    }

    public void close() {
        for (StreamHandler h : handlers) {
            h.close();
        }
        try {
            socket.close();
        } catch (Throwable ignored) {
        }
    }

    private class StreamHandler implements Runnable {
        /**
         * Set by the same thread that reads it.
         */
        private DataInputViewStreamWrapper inStream;

        /**
         * The socket for the specific stream.
         */
        private Socket connectedSocket;

        public StreamHandler(Socket clientSocket) {
            this.connectedSocket = clientSocket;
        }

        public void run() {
            boolean hasData = true;
            DataOutputStream out = null;
            while (hasData && !Thread.interrupted()) {
                try {
                    if (inStream == null) {
                        out = new DataOutputStream(connectedSocket.getOutputStream());
                        inStream = new DataInputViewStreamWrapper(connectedSocket.getInputStream());
                    }
                    System.out.println("waiting for message");
                    byte[] result = byteSerializer.deserialize(inStream);
                    System.out.println("result " + MessageType.getMessageType(result));
                    out.writeBytes("ack\n\r");
                    out.flush();
                    if(result == null) return;
                    queue.put(result);
                } catch (EOFException e) {
                    try {
                        out.flush();
                        out.close();
                        connectedSocket.close();
                    } catch (Throwable ignored) {
                    }

                    try {
                        socket.close();
                    } catch (Throwable ignored) {
                    }
                    return;
                } catch (Exception e) {
                    if (error == null) {
//           TODO:             throw e;
                        e.printStackTrace();
                        return;

                    } else {

                        // throw the root cause error
                        error.printStackTrace();
                        return;
//           TODO:             throw new Exception("Receiving stream failed: " + error.getMessage(), error);
                    }
                }
            }
        }

        public void close() {
            System.out.println("StreamHandler.close");
            if (connectedSocket != null) {
                try {
                    connectedSocket.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    private class ConnectionListener extends Thread {
        @Override
        public void run() {
            try {
                while (true) {
                    Socket newSocket = null;
                    newSocket = socket.accept();
                    new Thread(new StreamHandler(newSocket)).start();
                }
            } catch (SocketException e) {
                //    TODO: handle
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
