package org.flinkspector.core.runtime;


import com.google.common.primitives.Bytes;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to send output from Apache Flink to the listening runtime.
 */
public class OutputPublisher {

    private final ZMQ.Context context = ZMQ.context(1);
    private final ZMQ.Socket publisher;
	private AtomicInteger msgCount = new AtomicInteger(0);
	private AtomicBoolean opened = new AtomicBoolean(false);

    public OutputPublisher(String host, int port) {
        publisher = context.socket(ZMQ.PUSH);
		publisher.connect("tcp://127.0.0.1:" + port);
    }

	/**
	 * Send a opening message to the subscriber.
	 * Signaling the start of output.
	 * @param taskNumber index of the subtask.
	 * @param numTasks number of parallelism.
	 * @param serializer serialized serializer.
	 */
    public synchronized void sendOpen(int taskNumber,
                         int numTasks,
                         byte[] serializer) {
        String open = String.format("OPEN %d %d",
                taskNumber,
                numTasks);
        byte[] msg = Bytes.concat((open + " ;").getBytes(), serializer);
        publisher.send(msg);
	    opened.set(true);
    }

	/**
	 * Send a record message to the subscriber.
	 * @param bytes serialized record.
	 */
    public synchronized void sendRecord(byte[] bytes) {
		byte[] msg = Bytes.concat("REC".getBytes(), bytes);
	    msgCount.incrementAndGet();
        publisher.send(msg);
    }

	/**
	 * Signal the closing of the output producer.
	 * @param taskNumber index of the subtask.
	 */
    public synchronized void sendClose(int taskNumber) {
	    if(!opened.get()) {
		    //ignore misfire.
		    return;
	    }
	    String close = String.format("CLOSE %d %d",
                taskNumber, msgCount.get());
        publisher.send(close);
        publisher.close();
        context.term();
    }

}
