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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.flinkspector.core.trigger.VerifyFinishedTrigger;
import org.flinkspector.core.util.SerializeUtil;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Opens a 0MQ context and listens for output coming from a {@link OutputPublisher}.
 * This sink can run in parallel with multiple instances.
 * Feeds the {@link OutputVerifier} with the output and signals the result.
 *
 * @param <OUT> input type of the sink
 */
public class OutputHandler<OUT> implements Callable<OutputHandler.ResultState> {

	/**
	 * Verifier provided with output
	 */
	private final OutputVerifier<OUT> verifier;
	/**
	 * Number of parallel instances
	 */
	private int parallelism = -1;
	/**
	 * Set of parallel sink instances that have been started
	 */
	private final Set<Integer> participatingSinks = new HashSet<>();
	/**
	 * Set of finished parallel sink instances
	 */
	private final Set<Integer> closedSinks = new HashSet<>();
	/**
	 * Serializer to use for output
	 */
	TypeSerializer<OUT> typeSerializer = null;

	private List<byte[]> unserialized = new ArrayList<byte[]>();

	/**
	 * Number of records received
	 */
	private int numRecords = 0;
	/**
	 * Number of records reported
	 */
	private int expectedNumRecords = 0;

	private OutputSubscriber subscriber;

	/**
	 * Trigger
	 */
	private final VerifyFinishedTrigger<? super OUT> trigger;

	public OutputHandler(int port,
						 OutputVerifier<OUT> verifier,
						 VerifyFinishedTrigger<? super OUT> trigger) {

		subscriber = new OutputSubscriber(port);
		this.verifier = verifier;
		this.trigger = trigger;
	}

	public void close() {
		System.out.println("klappe zu");
		subscriber.close();
	}

	/**
	 * Listens for output from the test sink.
	 *
	 * @return {@link OutputHandler.ResultState}
	 * @throws FlinkTestFailedException
	 */
	public ResultState call() throws FlinkTestFailedException {
		Action nextStep = Action.STOP;
		// receive output from sink until finished all sinks are finished
		try {
			nextStep = processMessage(subscriber.getNextMessage());
			while (nextStep == Action.CONTINUE) {
				nextStep = processMessage(subscriber.getNextMessage());
			}
		} catch (EOFException e) {
			e.printStackTrace();
		} catch (IOException e) {
			throw new FlinkTestFailedException(e);
			//this means the socket was most likely closed forcefully by a timeout
		} catch (Exception e) {
			e.printStackTrace();
		}
		// close the connection
		subscriber.close();
		try {
			verifier.finish();
		} catch (Throwable e) {
			throw new FlinkTestFailedException(e);
		}
		// determine the final state of the operation
		if (nextStep == Action.FINISH) {
			return ResultState.SUCCESS;
		} else if (nextStep == Action.STOP) {
			return ResultState.TRIGGERED;
		} else {
			return ResultState.FAILURE;
		}
	}

	/**
	 * Signals the final state of the {@link OutputHandler}
	 * SUCCESS if the verification process has been finished.
	 * TRIGGERED if a trigger stopped the verification.
	 * FAILURE if the verification protocol was interrupted.
	 */
	public enum ResultState {
		TRIGGERED, SUCCESS, FAILURE
	}

	/**
	 * Signals the next step after calling <pre>processMessage()</pre>.
	 * CONTINUE if further messages are expected.
	 * STOP if the a trigger has fired.
	 * FINISH if all messages have been received.
	 */
	private enum Action {
		CONTINUE, STOP, FINISH
	}

	private boolean feedVerifier(byte[] bytes) throws IOException, FlinkTestFailedException {
		OUT elem = SerializeUtil.deserialize(bytes, typeSerializer);
		numRecords++;

		try {
			verifier.receive(elem);
		} catch (Exception e) {
			throw new FlinkTestFailedException(e);
		}

		return trigger.onRecord(elem) ||
				trigger.onRecordCount(numRecords);
	}

	/**
	 * Receives a byte encoded message.
	 * Determines the type of message, processes it
	 * and returns the next step.
	 *
	 * @param bytes byte representation of the msg.
	 * @return {@link Action} the next step.
	 * @throws IOException              if deserialization failed.
	 * @throws FlinkTestFailedException if the validation fails.
	 */
	private Action processMessage(byte[] bytes)
			throws IOException, FlinkTestFailedException {

		if(bytes == null) {
			System.out.println("Waited too long for message from sink");
			return Action.FINISH;
		}

		MessageType type = MessageType.getMessageType(bytes);

		String msg;
		byte[] out;

		switch (type) {
			case OPEN:
				System.out.println("OPEN");
				//Received a open message one of the sink instances
				//--> Memorize the index and the parallelism.
				if (participatingSinks.isEmpty()) {
					verifier.init();
				}
				msg = new String(bytes, "UTF-8");
				String[] values = msg.split(" ");
				participatingSinks.add(Integer.parseInt(values[1]));
				parallelism = Integer.parseInt(values[2]);
				if (typeSerializer == null) {
					out = type.getPayload(bytes);
					typeSerializer = SerializeUtil.deserialize(out);
					for(byte[] b: unserialized) {
						if(feedVerifier(b)) return Action.STOP;
					}
				}

				break;
			case REC:
				System.out.println("REC");
				//Received a record message from the sink.
				//--> call the verifier and the finishing trigger.
				out = type.getPayload(bytes);
				if(typeSerializer == null) {
					unserialized.add(out);
				} else {
					if (feedVerifier(out)) return Action.STOP;
				}
				break;
			case CLOSE:
				System.out.println("CLOSE");
				//Received a close message
				//--> register the index of the closed sink instance.
				msg = new String(bytes, "UTF-8");
				int sinkIndex = Integer.parseInt(msg.split(" ")[1]);
				int countRecords = Integer.parseInt(msg.split(" ")[2]);
				expectedNumRecords += countRecords;
				closedSinks.add(sinkIndex);
				break;
		}
		System.out.println("open sinks: " + (parallelism - closedSinks.size()));
		System.out.println("expected records: " + (expectedNumRecords - numRecords));
		//check if all sink instances have been closed.
		if (closedSinks.size() == parallelism &&
				numRecords == expectedNumRecords) {
			//finish the listening process
			return Action.FINISH;
		}
		return Action.CONTINUE;
	}
}


