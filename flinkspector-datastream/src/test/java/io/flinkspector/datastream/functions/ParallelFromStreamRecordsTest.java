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

import io.flinkspector.core.collection.ExpectedRecords;
import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.EventTimeInput;
import io.flinkspector.datastream.input.EventTimeInputBuilder;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.emptyIterableOf;

public class ParallelFromStreamRecordsTest extends DataStreamTestBase {

    @Test
    public void testFlushWindows() {
        EventTimeInput<Integer> input = EventTimeInputBuilder
                .startWith(1)
                .emit(1)
                .emit(1, intoWindow(2, minutes))
                .emit(2, after(30, seconds))
                .flushOpenWindowsOnTermination();


        DataStream<Integer> stream = createTestStream(input);

        DataStream<Integer> result = stream
                .timeWindowAll(Time.of(2, TimeUnit.MINUTES))
                .sum(0);

        ExpectedRecords<Integer> expected = ExpectedRecords
                .create(3)
                .expect(2);
        expected.refine().only();

        assertStream(result, expected);

    }

    @Test
    public void should_not_fire_window_yet() {
        DataStream<String> testStream = createTimedTestStreamWith("message1")
                .emit("message2")
                .close()
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        System.out.println(ctx.timestamp());;
                        out.collect(value);
                    }
                })
                .timeWindowAll(Time.days(1))
                .reduce((a, b) -> a + b);

        assertStream(testStream, emptyIterableOf(String.class));
    }
}
