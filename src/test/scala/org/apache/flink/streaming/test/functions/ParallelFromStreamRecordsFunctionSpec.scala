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

package org.apache.flink.streaming.test.functions

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.test.CoreSpec
import org.apache.flink.streaming.test.input.EventTimeInputBuilder
import org.apache.flink.streaming.test.input.time.After
import org.mockito.Mockito._

import scala.collection.JavaConversions._

class ParallelFromStreamRecordsFunctionSpec extends CoreSpec {

  val config = new ExecutionConfig()
  val typeInfo: TypeInformation[StreamRecord[String]] =
    TypeExtractor.getForObject(new StreamRecord[String]("t", 0))
  val serializer = typeInfo.createSerializer(config)

  "the source" should "produce input" in {
    val input = valuesToRecords(List("1", "2", "3", "4", "5")).toList
    val source = new ParallelFromStreamRecordsFunction[String](serializer, input)
    val streamContext = mock[StreamingRuntimeContext]
    when(streamContext.getNumberOfParallelSubtasks).thenReturn(1)
    when(streamContext.getIndexOfThisSubtask).thenReturn(0)

    source.setRuntimeContext(streamContext)
    val ctx = mock[SourceFunction.SourceContext[String]]
    when(ctx.getCheckpointLock).thenReturn(config, null)
    source.run(ctx)
    verify(ctx).collectWithTimestamp("1", 0)
    verify(ctx).collectWithTimestamp("2", 0)
    verify(ctx).collectWithTimestamp("3", 0)
    verify(ctx).collectWithTimestamp("4", 0)
    verify(ctx).collectWithTimestamp("5", 0)
  }

  it should "produce input in parallel" in {
    val input = valuesToRecords(List("1", "2", "3", "4", "5")).toList
    val source1 = new ParallelFromStreamRecordsFunction[String](serializer, input)
    val source2 = new ParallelFromStreamRecordsFunction[String](serializer, input)
    val streamContext1 = mock[StreamingRuntimeContext]
    when(streamContext1.getNumberOfParallelSubtasks).thenReturn(2)
    when(streamContext1.getIndexOfThisSubtask).thenReturn(0)
    val streamContext2 = mock[StreamingRuntimeContext]
    when(streamContext2.getNumberOfParallelSubtasks).thenReturn(2)
    when(streamContext2.getIndexOfThisSubtask).thenReturn(1)

    source1.setRuntimeContext(streamContext1)
    source2.setRuntimeContext(streamContext2)
    val ctx = mock[SourceFunction.SourceContext[String]]
    when(ctx.getCheckpointLock).thenReturn(config, config)
    source1.run(ctx)
    source2.run(ctx)
    verify(ctx).collectWithTimestamp("1", 0)
    verify(ctx).collectWithTimestamp("2", 0)
    verify(ctx).collectWithTimestamp("3", 0)
    verify(ctx).collectWithTimestamp("4", 0)
    verify(ctx).collectWithTimestamp("5", 0)
  }

  it should "emit continuously rising watermarks" in {
    val input = EventTimeInputBuilder.create("1")
      .emit("2", After.period(1, TimeUnit.SECONDS))
      .emit("3", After.period(1, TimeUnit.SECONDS))
      .emit("4", After.period(1, TimeUnit.SECONDS))
      .emit("5", After.period(1, TimeUnit.SECONDS))
      .emit("6", After.period(1, TimeUnit.SECONDS))
    val source = new ParallelFromStreamRecordsFunction[String](serializer, input.getInput)
    val streamContext = mock[StreamingRuntimeContext]
    when(streamContext.getNumberOfParallelSubtasks).thenReturn(1)
    when(streamContext.getIndexOfThisSubtask).thenReturn(0)
    source.setRuntimeContext(streamContext)
    val ctx = mock[SourceFunction.SourceContext[String]]
    when(ctx.getCheckpointLock).thenReturn(config, null)
    source.run(ctx)
    verify(ctx).collectWithTimestamp("1", 0)
    verify(ctx).collectWithTimestamp("2", 1000)
    verify(ctx).collectWithTimestamp("3", 2000)
    verify(ctx).collectWithTimestamp("4", 3000)
    verify(ctx).collectWithTimestamp("5", 4000)

    verify(ctx).emitWatermark(new Watermark(1000))
    verify(ctx).emitWatermark(new Watermark(2000))
    verify(ctx).emitWatermark(new Watermark(3000))
    verify(ctx).emitWatermark(new Watermark(4000))
  }

}
