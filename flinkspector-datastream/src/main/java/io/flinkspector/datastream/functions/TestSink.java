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

package io.flinkspector.datastream.functions;

import com.lmax.disruptor.RingBuffer;
import io.flinkspector.core.runtime.OutputEvent;
import io.flinkspector.core.runtime.OutputPublisher;
import io.flinkspector.core.util.SerializeUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Provides a sink that sends all incoming records using a 0MQ connection.
 * Starts sending data as soon as the first record is received.
 * Signals the end of data when the sink is closed.
 *
 * @param <IN> record type
 */
public class TestSink<IN> extends RichSinkFunction<IN> {

    /*
     * RingBuffer not serializable
     */
    private static RingBuffer<OutputEvent> buffer;
    private final int instance;
    private Logger LOG = LoggerFactory.getLogger(RichSinkFunction.class);
    private OutputPublisher handler;
    private TypeSerializer<IN> serializer;

    public TestSink(int instance, RingBuffer<OutputEvent> buffer) {
        this.instance = instance;
        TestSink.buffer = buffer;
    }


    @Override
    public void open(Configuration configuration) throws UnknownHostException {
        String jobManagerAddress = configuration
                .getString("jobmanager.rpc.address", "localhost");
        //open a socket to push data
        handler = new OutputPublisher(instance, buffer);
    }

    /**
     * Called when new data arrives at the sink.
     * Forwards the records via the 0MQ publisher.
     *
     * @param next incoming records
     */
    @Override
    public void invoke(IN next) {

        int numberOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int indexofThisSubTask = getRuntimeContext().getIndexOfThisSubtask();
        byte[] msg;

        if (serializer == null) {

            //startWith serializer
            TypeInformation<IN> typeInfo = TypeExtractor.getForObject(next);
            serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
            //push serializer to output receiver
            try {
                handler.sendOpen(indexofThisSubTask,
                        numberOfSubTasks,
                        SerializeUtil.serialize(serializer));

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
        handler.sendRecord(bytes);
    }

    @Override
    public void close() {
        //signal close to output receiver
        handler.sendClose(
                getRuntimeContext().getIndexOfThisSubtask());

    }

}
