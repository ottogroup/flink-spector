package org.flinkspector.scala.datastream

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


import org.apache.flink.api.scala._
import org.flinkspector.core.collection.ExpectedRecords
import org.flinkspector.core.input.InputBuilder
import org.flinkspector.core.quantify.{HamcrestVerifier, MatchRecords}
import org.hamcrest.{Description, TypeSafeDiagnosingMatcher}
import org.scalatest.Matchers
import org.scalatest.matchers.Matcher


class ProductMatcher[T <: Product, V](key: String,
                                      matcher: Matcher[V],
                                      mask: List[String])
  extends TypeSafeDiagnosingMatcher[T] {

  def resolve(key: String, product: Product): V = {
    product.productElement(mask.indexOf(key)).asInstanceOf[V]
  }

  override def matchesSafely(item: T, mismatchDescription: Description): Boolean = {
    val description = new StringBuilder

    val result = matcher.apply(resolve(key, item))
    if (!result.matches) {
      description ++= s"<$key> "
      description ++= result.failureMessage
      mismatchDescription.appendText(description.toString())
      false
    } else {
      true
    }
  }

  override def describeTo(description: Description): Unit = {
  }
}

class MatchProduct[T <: Product](mask: List[String])
  extends MatchRecords[T] with Matchers {

  case class Test3()

  def assertThat[V](key: String, matcher: Matcher[V]): MatchProduct[T] = {
    super.assertThat(new ProductMatcher[T, V](key, matcher, mask))
    this
  }

}

case class Test(s: String)

class DataStreamTest extends CoreSpec {

  "basic test" should "do shit" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val stream = env.fromCollection(List(1, 2, 3, 4)).map(_ + 1)

    val expected = ExpectedRecords.create(2).expect(3).expect(4).expect(5)
    val verifier = new HamcrestVerifier[Int](expected)

    val sink = env.createTestSink(verifier)

    stream.addSink(sink)

    env.executeTest()
  }

  "assert test" should "do shi" in {

    import scala.reflect.runtime.{universe => u}

    def companionMembers(clazzTag: scala.reflect.ClassTag[_]): u.MemberScope = {
      val runtimeClass = clazzTag.runtimeClass
      val rootMirror = u.runtimeMirror(runtimeClass.getClassLoader)
      val classSymbol = rootMirror.classSymbol(runtimeClass)
      // get the companion here
      println(classSymbol.typeSignature.members)

      println(classSymbol.companion)

      classSymbol.companion.typeSignature.members
    }

    case class MyClass(value: Int)

    println(companionMembers(scala.reflect.classTag[MyClass]))

    val applyMethods =
      companionMembers(scala.reflect.classTag[MyClass])
        .filter { m => m.isMethod && m.name.toString == "apply"}

    println(applyMethods)

    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val input = InputBuilder.startWith(("test", 9))
      .emit(("check", 5))
      .emit(("check", 6))
      .emit(("check", 11))
      .emit(("check", 23))

    val expected = new MatchProduct[(String, Int)](List("key", "value"))
      .assertThat("key", be(10))
      .assertThat("key", be > 10)

    case class Foo(key: String, value: Int)
    case class Bar(key: String)

    val tuple = ("test",1)





  }

}


