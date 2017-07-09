package org.flinkspector.core.runtime;


import com.google.common.primitives.Bytes;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to send output from Apache Flink to the listening runtime.
 */
public class OutputPublisher {

//	private final ZMQ.Context context = ZMQ.context(1);
//	private final ZMQ.Socket publisher;
	private AtomicInteger msgCount = new AtomicInteger(0);
	private AtomicBoolean opened = new AtomicBoolean(false);

	private Socket client;
	private OutputStream outputStream;
	private DataOutputViewStreamWrapper streamWriter;

	TypeSerializer<byte[]> serializer;
	private InetAddress hostAdress;
	private int port;

	private boolean socketOpen = false;

	public OutputPublisher(String host, int port) throws UnknownHostException {
		hostAdress = InetAddress.getLocalHost();
		this.port = port;
		//TODO: use real config
		ExecutionConfig config = new ExecutionConfig();
		TypeInformation<byte[]> typeInfo = TypeExtractor.getForObject(new byte[0]);
		serializer = typeInfo.createSerializer(config);
	}

	private void open() {
		if(!socketOpen) {
			try {
				client = new Socket(hostAdress, port);
				outputStream = client.getOutputStream();
				streamWriter = new DataOutputViewStreamWrapper(outputStream);
			} catch (IOException e) {
				System.out.println("Error connecting to " + hostAdress + ":" + port );
				e.printStackTrace();
			}
			socketOpen = true;
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
		open();
		String open = String.format("OPEN %d %d",
				taskNumber,
				numTasks);
		byte[] msg = Bytes.concat((open + " ;").getBytes(), serializer);

//		publisher.send(msg);
		sendBytes(msg);
		opened.set(true);
	}

	private void sendBytes(byte[] bytes) {
		try {
			serializer.serialize(bytes,streamWriter);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Send a record message to the subscriber.
	 *
	 * @param bytes serialized record.
	 */
	public synchronized void sendRecord(byte[] bytes) {
		byte[] msg = Bytes.concat("REC".getBytes(), bytes);
		msgCount.incrementAndGet();
		sendBytes(msg);
//		publisher.send(msg);
	}

	/**
	 * Signal the closing of the output producer.
	 *
	 * @param taskNumber index of the subtask.
	 */
	public synchronized void sendClose(int taskNumber) {
		if (!opened.get()) {
			//ignore misfire.
			return;
		}
		String close = String.format("CLOSE %d %d",
				taskNumber, msgCount.get());

		sendBytes(close.getBytes());
		try {
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		publisher.send(close);
//		publisher.close();
//		context.term();
	}

	public void close() throws Exception {
		try {
			if (outputStream != null) {
				outputStream.flush();
				outputStream.close();
			}

			// first regular attempt to cleanly close. Failing that will escalate
			if (client != null) {
				client.close();
			}
		}
		catch (Exception e) {
			throw new IOException("Error while closing connection that streams data back to client at ");
//					+ hostIp.toString() + ":" + port, e);
		}
		finally {
			// if we failed prior to closing the client, close it
			if (client != null) {
				try {
					client.close();
				}
				catch (Throwable t) {
					// best effort to close, we do not care about an exception here any more
				}
			}
		}
	}

}
