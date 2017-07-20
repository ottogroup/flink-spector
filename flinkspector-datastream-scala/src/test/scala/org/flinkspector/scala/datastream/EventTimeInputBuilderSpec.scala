package org.flinkspector.scala.datastream

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.flinkspector.scala.datastream.CoreSpec
import org.flinkspector.datastream.input.EventTimeInputBuilder
import org.flinkspector.datastream.input.time.After
import scala.collection.JavaConversions._

class EventTimeInputBuilderSpec extends CoreSpec {

  trait EventTimeInputBuilderCase {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, After.period(2, TimeUnit.SECONDS))
      .emit(3, After.period(1, TimeUnit.SECONDS))
      .emit(4, After.period(3, TimeUnit.SECONDS))

  }

  "the builder" should "repeat a input list one times" in
    new EventTimeInputBuilderCase {
      builder.repeatAll(After.period(2, TimeUnit.SECONDS), 1)
      builder.getInput.toList shouldBe List(
        new StreamRecord[Int](1, 0),
        new StreamRecord[Int](2, 2000),
        new StreamRecord[Int](3, 3000),
        new StreamRecord[Int](4, 6000),
        new StreamRecord[Int](1, 8000),
        new StreamRecord[Int](2, 10000),
        new StreamRecord[Int](3, 11000),
        new StreamRecord[Int](4, 14000)
      )
    }

}
