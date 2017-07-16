package org.flinkspector.core.runtime

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.scalatest.{FlatSpec, Matchers}

class ByteEventTranslatorSpec extends FlatSpec with Matchers {

  "The translator" should "translate a message" in {
    val translator = new ByteEventTranslator()

    val message = "hello".getBytes(StandardCharsets.UTF_8)
    val bb = ByteEventTranslator.translateToBuffer(1, message)

    val event = new ByteEvent()

    translator.translateTo(event,1,bb)

    new String(event.getMsg(), StandardCharsets.UTF_8) shouldBe "hello"

  }

}
