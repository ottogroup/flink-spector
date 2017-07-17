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

package org.flinkspector.dataset.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.flinkspector.core.input.Input;
import org.flinkspector.core.input.InputBuilder;
import org.flinkspector.core.input.InputTranslator;
import org.flinkspector.dataset.DataSetTestBase;

import static java.util.Arrays.asList;

public class InputDefinitionTest extends DataSetTestBase {

    private DataSet<Tuple2<String, Integer>> swap(DataSet<Tuple2<String, Integer>> set) {
        return set.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return value;
            }
        });
    }

    public void myTest() {

        Input<String> stringInput = InputBuilder
                .startWith("{ \"name\":\"hans\", \"age\":65, \"ts\":12, \"id\":\"joto12345\"}")
                .emit("{ \"name\":\"fritz\", \"age\":\"12\", \"ts\":12, \"id\":\"joto12345\"}")
                .emit("{ \"name\":\"rose\", \"age\":\"21\", \"ts\":12, \"id\":\"joto12345\"}")
                .emit("{ \"name\":\"samanta\", \"age\":\"45\", \"ts\":12, \"id\":\"joto12345\"}");


        DataSet<String> dataSet1 = createTestDataSet(asList("fritz", "peter", "hans"));

        Input<Tuple2<String, Integer>> input = InputBuilder
                .startWith(Tuple2.of("one", 1))
                .emit(Tuple2.of("two", 2))
                .emit(Tuple2.of("three", 3))
                .repeatAll(times(2))
                .emit(Tuple2.of("four", 3), times(3));

        Input<String> translatedInput = new TupleToJsonString(input);

        DataSet<Tuple2<String, Integer>> dataSet2 = createTestDataSet(input);

        DataSet<Tuple2<String, Integer>> dataSet3 = createTestDataSetWith(Tuple2.of("one", 1))
                .emit(Tuple2.of("two", 2))
                .emit(Tuple2.of("three", 3))
                .repeatAll(times(2))
                .emit(Tuple2.of("four", 3), times(3))
                .close();

    }

    private class TupleToJsonString
            extends InputTranslator<Tuple2<String, Integer>, String> {

        public TupleToJsonString(Input<Tuple2<String, Integer>> input) {
            super(input);
        }

        @Override
        protected String translate(Tuple2<String, Integer> elem) {
            return "{ \"name\":\"" +
                    elem.f0 +
                    "\", \"age\":" +
                    elem.f1 +
                    ", \"ts\":12, \"id\":\"joto12345\"}";
        }
    }

}
