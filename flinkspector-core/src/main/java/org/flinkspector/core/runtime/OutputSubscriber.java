package org.flinkspector.core.runtime;


import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.io.FileDescriptor.out;

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
        System.out.println("waiting...");
        return queue.take();
    }

    public String recvStr() throws Exception {
        return new String(readNextFromStream(), StandardCharsets.UTF_8);
    }

    public OutputSubscriber(int port) {

        ExecutionConfig config = new ExecutionConfig();
        TypeInformation<byte[]> typeInfo = TypeExtractor.getForObject(new byte[0]);
        byteSerializer = typeInfo.createSerializer(config);

        try {
            socket = new ServerSocket(port, 1);
        } catch (IOException e) {
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
            while (hasData && !Thread.interrupted()) {
                try {
                    if (inStream == null) {
                        inStream = new DataInputViewStreamWrapper(connectedSocket.getInputStream());
                    }
                    //check if there is actually data in the stream
//                if(inStream.available() == 0) return null;

                    queue.put(byteSerializer.deserialize(inStream));
                } catch (EOFException e) {
                    try {
                        connectedSocket.close();
                    } catch (Throwable ignored) {
                    }

                    try {
                        socket.close();
                    } catch (Throwable ignored) {
                    }
                    hasData = false;
                } catch (Exception e) {
                    if (error == null) {
//           TODO:             throw e;
                    } else {
                        // throw the root cause error
//           TODO:             throw new Exception("Receiving stream failed: " + error.getMessage(), error);
                    }
                }
            }
        }

        public void close() {
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
                    System.out.println("new socket");
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
