package org.apache.flink.streaming.test.tool.runtime.input

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.test.tool.CoreSpec
import org.mockito.Mockito._

import scala.collection.JavaConversions._

class ParallelFromStreamRecordsFunctionSpec extends CoreSpec {

  val config = new ExecutionConfig()
  val typeInfo: TypeInformation[StreamRecord[String]] =
    TypeExtractor.getForObject(new StreamRecord[String]("t",0))
  val serializer = typeInfo.createSerializer(config)

  "the source" should "produce input" in {
    val input = valuesToRecords(List("1","2","3","4","5")).toList
    val source = new ParallelFromStreamRecordsFunction[String](serializer,input)
    val streamContext = mock[StreamingRuntimeContext]
    when(streamContext.getNumberOfParallelSubtasks).thenReturn(1)
    when(streamContext.getIndexOfThisSubtask).thenReturn(0)

    source.setRuntimeContext(streamContext)
    val ctx = mock[SourceFunction.SourceContext[String]]
    when(ctx.getCheckpointLock).thenReturn(config,null)
    source.run(ctx)
    verify(ctx).collectWithTimestamp("1",0)
    verify(ctx).collectWithTimestamp("2",0)
    verify(ctx).collectWithTimestamp("3",0)
    verify(ctx).collectWithTimestamp("4",0)
    verify(ctx).collectWithTimestamp("5",0)
  }

  it should "produce input in parallel" in {
    val input = valuesToRecords(List("1","2","3","4","5")).toList
    val source1 = new ParallelFromStreamRecordsFunction[String](serializer,input)
    val source2 = new ParallelFromStreamRecordsFunction[String](serializer,input)
    val streamContext1 = mock[StreamingRuntimeContext]
    when(streamContext1.getNumberOfParallelSubtasks).thenReturn(2)
    when(streamContext1.getIndexOfThisSubtask).thenReturn(0)
    val streamContext2 = mock[StreamingRuntimeContext]
    when(streamContext2.getNumberOfParallelSubtasks).thenReturn(2)
    when(streamContext2.getIndexOfThisSubtask).thenReturn(1)

    source1.setRuntimeContext(streamContext1)
    source2.setRuntimeContext(streamContext2)
    val ctx = mock[SourceFunction.SourceContext[String]]
    when(ctx.getCheckpointLock).thenReturn(config,config)
    source1.run(ctx)
    source2.run(ctx)
    verify(ctx).collectWithTimestamp("1",0)
    verify(ctx).collectWithTimestamp("2",0)
    verify(ctx).collectWithTimestamp("3",0)
    verify(ctx).collectWithTimestamp("4",0)
    verify(ctx).collectWithTimestamp("5",0)
  }

}
