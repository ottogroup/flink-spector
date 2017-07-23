package org.flinkspector.scala.datastream

import java.util
import scala.collection.JavaConverters._

import org.flinkspector.core.runtime.{FlinkTestFailedException, SimpleOutputVerifier}

/**
  * Created by willi on 17.07.17.
  */
class FunctionVerifier[T](val function: Iterable[T] => Any) extends SimpleOutputVerifier[T] {

  @throws[FlinkTestFailedException]
  override def verify(output: util.List[T]): Unit = {
    function(output.asScala)
  }
}

