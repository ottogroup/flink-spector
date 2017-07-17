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

package org.flinkspector.datastream.functions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.flinkspector.core.collection.ExpectedRecords;
import org.flinkspector.datastream.DataStreamTestBase;
import org.flinkspector.datastream.input.EventTimeInput;
import org.flinkspector.datastream.input.EventTimeInputBuilder;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

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
}
