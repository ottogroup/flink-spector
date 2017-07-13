package org.flinkspector.core.runtime;


import com.google.common.primitives.Bytes;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to send output from Apache Flink to the listening runtime.
 */
public class OutputPublisher {


    Vertx vertx = Vertx.vertx();

    private AtomicInteger msgCount = new AtomicInteger(0);
    private Set<Integer> closed = new HashSet<Integer>();
    private int port;
    private String host;

    private int parallelism = -1;

    private NetClient client;
    private NetSocket socket;

    private Queue<byte[]> queue = new ConcurrentLinkedQueue<byte[]>();

    private Future<NetSocket> socketFuture;


    public OutputPublisher(String host, int port) throws UnknownHostException {
        this.host = host;
        this.port = port;

        NetClientOptions options = new NetClientOptions()
                .setConnectTimeout(10000)
                .setReconnectAttempts(10)
                .setReconnectInterval(500)
                .setLogActivity(true);

        client = vertx.createNetClient(options);
        connect();
    }


    public void close() {
        client.close();
        vertx.close();
    }

    /**
     * Send a opening message to the subscriber.
     * Signaling the start of output.
     *
     * @param taskNumber index of the subtask.
     * @param numTasks   number of parallelism.
     * @param serializer serialized serializer.
     */
    public void sendOpen(int taskNumber,
                         int numTasks,
                         byte[] serializer) {
        String open = String.format("OPEN %d %d",
                taskNumber,
                numTasks);
        byte[] msg = Bytes.concat((open + " ;").getBytes(), serializer);
        parallelism = numTasks;
        queueMessage(msg);
    }

    private Buffer packageMessage(byte[] bytes) {
        Buffer b = Buffer.buffer(bytes);
        return Buffer
                .buffer()
                .appendInt(b.length())
                .appendBuffer(b);

    }

    private void connect() {
        socketFuture = Future.future();

        client.connect(port, "localhost", res -> {
            if (res.succeeded()) {
                socket = res.result();
                socket.closeHandler(c ->
                        System.out.println("client socket closed!")
                );
                byte[] request = queue.poll();
                while (request != null) {
                    socket.write(packageMessage(request));
                    request = queue.poll();
                }
                socketFuture.complete(socket);
            } else {
                System.out.println("Failed to connect: " + res.cause().getMessage());
            }
        });
    }

    private void queueMessage(byte[] bytes) {
        if(socketFuture.isComplete()) {
            socket.write(packageMessage(bytes));
        } else {
            queue.add(bytes);
        }
    }

    public void send(byte[] bytes) {
        queueMessage(bytes);
    }

    public void send(String string) {
        queueMessage(string.getBytes(StandardCharsets.UTF_8));
    }


    /**
     * Send a record message to the subscriber.
     *
     * @param bytes serialized record.
     */
    public void sendRecord(byte[] bytes) {
        byte[] msg = Bytes.concat("REC".getBytes(), bytes);
        msgCount.incrementAndGet();
        queueMessage(msg);
    }

    /**
     * Signal the closing of the output producer.
     *
     * @param taskNumber index of the subtask.
     */
    public void sendClose(int taskNumber) {
//        System.out.println("close taskNumber = " + taskNumber);
        if (!closed.contains(taskNumber)) {
            String close = String.format("CLOSE %d %d",
                    taskNumber, msgCount.get());

            queueMessage(close.getBytes());
            closed.add(taskNumber);
        }
    }




}
