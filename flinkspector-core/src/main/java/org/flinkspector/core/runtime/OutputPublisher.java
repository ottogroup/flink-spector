package org.flinkspector.core.runtime;


import com.google.common.primitives.Bytes;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to send output from Apache Flink to the listening runtime.
 */
public class OutputPublisher {

    private BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(1000);

    private AtomicInteger msgCount = new AtomicInteger(0);
    private Set<Integer> closed = new HashSet<Integer>();
    private int port;
    private String host;

    private int parallelism = -1;

    private Thread sender;

    public OutputPublisher(String host, int port) throws UnknownHostException {
        this.host = host;
        this.port = port;
        startSending();
    }

    public void startSending() {
        sender = new Thread(new OutputPub(host,port));
        sender.start();
    }

    public void close() {
        sender.interrupt();
    }

    /**
     * Send a opening message to the subscriber.
     * Signaling the start of output.
     *
     * @param taskNumber index of the subtask.
     * @param numTasks   number of parallelism.
     * @param serializer serialized serializer.
     */
    public synchronized void sendOpen(int taskNumber,
                                      int numTasks,
                                      byte[] serializer) {
        String open = String.format("OPEN %d %d",
                taskNumber,
                numTasks);
        byte[] msg = Bytes.concat((open + " ;").getBytes(), serializer);
        parallelism = numTasks;
        queueMessage(msg);
    }

    private void queueMessage(byte[] bytes) {
        try {
            queue.put(bytes);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
    public synchronized void sendRecord(byte[] bytes) {
        byte[] msg = Bytes.concat("REC".getBytes(), bytes);
        msgCount.incrementAndGet();
        queueMessage(msg);
    }

    /**
     * Signal the closing of the output producer.
     *
     * @param taskNumber index of the subtask.
     */
    public synchronized void sendClose(int taskNumber) {
        if (!closed.contains(taskNumber)) {
            String close = String.format("CLOSE %d %d",
                    taskNumber, msgCount.get());

            queueMessage(close.getBytes());
            closed.add(taskNumber);
        }
//        if(closed.size() == parallelism) {
//            try {
//                close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
    }



    private class OutputPub implements Runnable {

        private AtomicBoolean socketOpen = new AtomicBoolean(false);
        BufferedReader brinp = null;
        InputStream inp = null;
        private static final int RETRIES = 3;

        private Socket client;
        private OutputStream outputStream;
        private DataOutputViewStreamWrapper streamWriter;

        TypeSerializer<byte[]> serializer;
        private InetAddress hostAdress;
        private int port;

        OutputPub(String host, int port) {
            //TODO: hostAddress from constructor
            hostAdress = InetAddress.getLoopbackAddress();
            this.port = port;
            //TODO: use real config
            ExecutionConfig config = new ExecutionConfig();
            TypeInformation<byte[]> typeInfo = TypeExtractor.getForObject(new byte[0]);
            serializer = typeInfo.createSerializer(config);
        }


        private synchronized void open() {
            if (!socketOpen.getAndSet(true)) {
                try {
                    connect();
                } catch (SocketException e) {
                    reconnect(RETRIES);
                } catch (IOException e) {
                    System.out.println("Error while opening socket " + e);
                }
            }
        }

        private void connect() throws IOException {
            client = new Socket(hostAdress, port);
            outputStream = client.getOutputStream();
            inp = client.getInputStream();
            brinp = new BufferedReader(new InputStreamReader(inp));
            streamWriter = new DataOutputViewStreamWrapper(outputStream);
        }

        private synchronized void sendMessage(byte[] bytes) {
            open();
            try {
                sendBytes(bytes);
            } catch (SocketException e) {
                System.out.println("sending failed");
                retry(bytes, RETRIES);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        private void sendBytes(byte[] bytes) throws IOException {
            serializer.serialize(bytes, streamWriter);
            outputStream.flush();
            // wait for ack
            brinp.readLine();
        }

        private void retry(byte[] bytes, int times) {
            if (times == 0) {
                throw new IllegalStateException("message could not be sent!");
                //do nothing
            } else {
                try {
                    Thread.sleep(10 ^ (RETRIES - times));
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                try {
                    close();
                    open();
                    sendBytes(bytes);
                } catch (IOException e) {
                    retry(bytes, --times);
                }
            }
        }

        private void reconnect(int times) {
            if (times == 0) {
                throw new IllegalStateException("Could not connect to socket");
                //do nothing
            } else {
                try {
                    Thread.sleep(10 ^ (RETRIES - times));
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                try {
                    close();
                    connect();
                } catch (IOException e) {
                    reconnect(--times);
                }
            }
        }

        public synchronized void close() throws IOException {
            System.out.println("closing publisher");
            try {
                if (outputStream != null) {
                    outputStream.flush();
                    outputStream.close();
                }
                if (inp != null) {
                    brinp.close();
                    inp.close();
                }

                // first regular attempt to cleanly close. Failing that will escalate
                if (client != null) {
                    client.close();
                }
            } catch (Exception e) {
                throw new IOException("Error while closing connection that streams data back to client at "
                        + hostAdress.toString() + ":" + port, e);
            } finally {
                socketOpen.set(false);
                // if we failed prior to closing the client, close it
                if (client != null) {
                    try {
                        client.close();
                    } catch (Throwable t) {
                        // best effort to close, we do not care about an exception here any more
                    }
                }
            }
        }

        @Override
        public void run() {
            open();
            while (!Thread.interrupted()) {
                try {
                    byte[] msg = queue.take();
                    System.out.println("msg = " + msg);
                    sendMessage(msg);
                } catch (InterruptedException e) {
                    System.out.println("interrupt");
                    break;
                }
            }
            System.out.println("stop sending!");
            try {
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
