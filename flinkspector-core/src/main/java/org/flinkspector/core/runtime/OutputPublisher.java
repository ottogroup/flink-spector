package org.flinkspector.core.runtime;


import com.google.common.primitives.Bytes;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to send output from Apache Flink to the listening runtime.
 */
public class OutputPublisher {

    private final RingBuffer<ByteEvent> ringBuffer;

    private int instance;

    private static final EventTranslatorOneArg<ByteEvent, ByteBuffer> TRANSLATOR =
            new ByteEventTranslator();

    public void onData(ByteBuffer bb)
    {
        ringBuffer.publishEvent(TRANSLATOR, bb);
    }

    private AtomicInteger msgCount = new AtomicInteger(0);
    private Set<Integer> closed = new HashSet<Integer>();

    public OutputPublisher(int instance, RingBuffer<ByteEvent> buffer) {
        ringBuffer = buffer;
        this.instance = instance;
    }


    public void close() {

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
        queueMessage(msg);
    }

    private void queueMessage(byte[] bytes) {
      ByteBuffer bb = ByteEventTranslator.translateToBuffer(instance, bytes);
      ringBuffer.publishEvent(TRANSLATOR, bb);
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
