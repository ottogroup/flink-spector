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

package org.flinkspector.scala.datastream

import org.hamcrest.{Description, TypeSafeDiagnosingMatcher}
import scala.collection.mutable.ArrayBuffer
import java.lang.{Iterable => JIterable}
import scala.collection.JavaConversions._

class AssertBlock[T <: Product : Manifest]
  extends TypeSafeDiagnosingMatcher[JIterable[_ <: Product]] {

  var v: T = _

  val assertions = ArrayBuffer.empty[() => Unit]

  def assertThat[V](assert: => Unit) = {
    assertions += (() => assert)
  }

  def valid(product: Product): Option[String] = {
    v = productToCaseClass[T](product)
    assertions.foreach { assert =>
      try {
        assert()
      } catch {
        case t: Throwable =>
          return Some(t.toString)
      }
    }
    None
  }

  override def matchesSafely(iterable: JIterable[_ <: Product], mismatchDescription: Description): Boolean = {
    iterable.foreach { item =>
      valid(item) match {
        case None => true
        case Some(s: String) =>
          mismatchDescription.appendText(s)
          return false
      }
    }
    true
  }

  override def describeTo(description: Description): Unit = {

  }

  def productToCaseClass[P <: Product](product: Product)(implicit manifest: Manifest[P]): P = {
    val runtimeClass = manifest.runtimeClass
    val innerCaseClass = """(.*\$)([0-9]+)$""".r

    val caseClassCompanionName: String = innerCaseClass findFirstIn runtimeClass.getName match {
      case Some(innerCaseClass(name, index)) =>
        throw new IllegalArgumentException(s"class $name+$index is not accessable from this context")
      case None => runtimeClass.getName + "$"
    }

    val companionObjectClass = Class.forName(caseClassCompanionName)
    val ccCompanionConstructor = companionObjectClass.getDeclaredConstructor()
    ccCompanionConstructor.setAccessible(true)
    val ccCompanionObject = ccCompanionConstructor.newInstance()

    val applyMethod = companionObjectClass
      .getMethods
      .find(x => x.getName == "apply" && x.isBridge).get

    val values = (product.productIterator map (_.asInstanceOf[AnyRef])).toSeq

    val caseClass: P = applyMethod.invoke(ccCompanionObject, values: _*)
      .asInstanceOf[P]

    caseClass
  }

}


