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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to send output from Apache Flink to the listening runtime.
 */
public class OutputPublisher {

    private AtomicInteger msgCount = new AtomicInteger(0);
    private Set<Integer> closed = new HashSet<Integer>();

    private Socket client;
    private OutputStream outputStream;
    private DataOutputViewStreamWrapper streamWriter;

    TypeSerializer<byte[]> serializer;
    private InetAddress hostAdress;
    private int port;
    private int parallelism = -1;
    BufferedReader brinp = null;
    InputStream inp = null;

    private AtomicBoolean socketOpen = new AtomicBoolean(false);

    public OutputPublisher(String host, int port) throws UnknownHostException {
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
                client = new Socket(hostAdress, port);
                outputStream = client.getOutputStream();
                inp = client.getInputStream();
                brinp = new BufferedReader(new InputStreamReader(inp));
                streamWriter = new DataOutputViewStreamWrapper(outputStream);
            } catch (IOException e) {
                System.out.println("Error while opening socket " + e);
            }
        }
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
        sendMessage(msg);
    }

    private synchronized void sendMessage(byte[] bytes) {
        open();
        try {
            sendBytes(bytes);
        } catch (SocketException e) {
            System.out.println("sending failed");
            retry(bytes,3);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void sendBytes(byte[] bytes) throws IOException {
        serializer.serialize(bytes, streamWriter);
        // wait for ack
        brinp.readLine();
    }

    private void retry(byte[] bytes, int times) {
        if(times == 0) {
            throw new IllegalStateException("message could not be sent!");
            //do nothing
        } else {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            try {
                close();
                open();
                sendBytes(bytes);
            } catch (IOException e) {
                retry(bytes, --times);
                e.printStackTrace();
            }
        }
    }


    public void send(byte[] bytes) {
        sendMessage(bytes);
    }

    public void send(String string) {
        sendMessage(string.getBytes(StandardCharsets.UTF_8));
    }


    /**
     * Send a record message to the subscriber.
     *
     * @param bytes serialized record.
     */
    public synchronized void sendRecord(byte[] bytes) {
        byte[] msg = Bytes.concat("REC".getBytes(), bytes);
        msgCount.incrementAndGet();
        sendMessage(msg);
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

            sendMessage(close.getBytes());
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

    public synchronized void close() throws IOException {
        System.out.println("closing publisher");
        try {
            if (outputStream != null) {
                outputStream.flush();
                outputStream.close();
            }
            if(inp != null) {
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

}
