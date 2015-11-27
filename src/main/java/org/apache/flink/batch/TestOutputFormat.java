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

package org.apache.flink.batch;

import com.google.common.primitives.Bytes;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.core.util.SerializeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.IOException;

public class TestOutputFormat<IN> extends RichOutputFormat<IN> {

	private static final long serialVersionUID = 1L;

	private transient ZMQ.Context context;
	private transient ZMQ.Socket publisher;
	private TypeSerializer<IN> serializer;
	private final int port;
	private String jobManagerAddress;

	private int taskNumber;
	private int numTasks;

	private Logger LOG = LoggerFactory.getLogger(RichSinkFunction.class);

	public TestOutputFormat(int port) {
		this.port = port;
	}

	@Override
	public void configure(Configuration configuration) {
		jobManagerAddress = configuration
				.getString("jobmanager.rpc.address", "localhost");
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.taskNumber = taskNumber;
		this.numTasks = numTasks;

		//open a socket to push data
		context = ZMQ.context(1);
		publisher = context.socket(ZMQ.PUSH);
		publisher.connect("tcp://" + jobManagerAddress + ":" + port);
	}

	@Override
	public void writeRecord(IN next) throws IOException {
		byte[] msg;
		if (serializer == null) {

			//transmit parallelism
			publisher.send(String.format("OPEN %d %d",
					taskNumber,
					numTasks), 0);
			//create serializer
			TypeInformation<IN> typeInfo = TypeExtractor.getForObject(next);
			serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
			//push serializer to output receiver
			try {
				msg = Bytes.concat("SER".getBytes(), SerializeUtil.serialize(serializer));
				publisher.send(msg, 0);
			} catch (IOException e) {
				LOG.error("Could not serialize TypeSerializer", e);
				return;
			}
		}


		//serialize input and push to output
		byte[] bytes;
		try {
			bytes = SerializeUtil.serialize(next, serializer);
		} catch (IOException e) {
			LOG.error("Could not serialize input", e);
			return;
		}
		msg = Bytes.concat("REC".getBytes(), bytes);
		publisher.send(msg, 0);
	}

	@Override
	public void close() throws IOException {
		//signal close to output receiver
		String end = String.format("CLOSE %d",
				taskNumber);
		publisher.send(end, 0);
		publisher.close();
		context.term();
	}
}
