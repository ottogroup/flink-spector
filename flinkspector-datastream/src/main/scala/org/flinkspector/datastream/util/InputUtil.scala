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

package org.flinkspector.datastream.util

import java.io.IOException
import java.lang.{Iterable => JIterable, Long => JLong}
import java.util.{List => JList}

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object InputUtil {
  /**
    * Calculates the watermarks for a list of
    * [[org.apache.flink.streaming.runtime.streamrecord.StreamRecord]]s.
    * <p>
    * Produces a list of longs with the same length as the input list,
    * defining for each record if a watermark with what value can be emitted.
    * If a long is negative no watermark can be emitted at this point.
    *
    * e.g.: A input with timestamps: (3, 1, 11, 2, 5, 4, 10, 8, 7, 9)
    * will return List(-1,1,-1,3,-1,5,-1,-1,8,11)
    *
    * @param records list of [[StreamRecord]]s
    * @tparam T type of the values
    * @return list of watermarks
    */
  def calculateWatermarks[T](records: java.lang.Iterable[StreamRecord[T]],
                             lastValueMax: Boolean = false): JList[JLong] = {
    val timestamps = records.map(_.getTimestamp)
    if (timestamps.size != records.size) {
      throw new IOException("The list of watermarks has not the same length as the output")
    }
    produceWatermarks(timestamps.toList, lastValueMax)
  }

  /**
    * Implicit conversion from a[[List]]] of [[Long]]
    * to a  [[JList]] of [[JLong]]
    *
    * @param lst
    * @return
    */
  implicit def toLongList(lst: List[Long]): JList[JLong] =
    seqAsJavaList(lst.map(i => i: java.lang.Long))

  /**
    * Calculates the watermarks for a list of [[Long]].
    * <p>
    * Produces a list of longs with the same length as the input list,
    * defining for each record if a watermark with what value can be emitted.
    * If a long is negative no watermark can be emitted at this point.
    *
    * e.g.: A input with timestamps: (3, 1, 11, 2, 5, 4, 10, 8, 7, 9)
    * will return List(-1,1,-1,3,-1,5,-1,-1,8,11)
    *
    * @param timestamps list of [[Long]]
    * @return list of watermarks
    */
  def produceWatermarks(timestamps: List[Long],
                        lastValueMax: Boolean = false): List[Long] = {
    val max = if (lastValueMax) {
      Long.MaxValue
    } else {
      timestamps.max
    }
    val array = new ArrayBuffer[Long]()
    val seen = new ArrayBuffer[Long]()

    def recur(l: List[Long]): List[Long] = {
      if (l.isEmpty) {
        return l
      }
      //TODO refactor recursion
      val ts = l.head
      if (l.count(ts >= _) == 1) {
        val watermark =
          if (l.tail.nonEmpty) {
            val wm = Try(seen.filter(_ < l.tail.min).max)
              .getOrElse(ts)
            if (ts >= wm) {
              ts
            } else {
              wm
            }
          } else {
            max
          }
        array += watermark
      } else {
        array += -1
      }
      seen += ts
      recur(l.tail)
    }
    recur(timestamps)
    array.toList
  }

  /**
    * Splits a [[JList]] into partitions and returns one partition.
    * <p>
    * List(1,2,3,4) with two partitions will be turned into
    * List(1,3) and List(2,4).
    *
    * @param input         list to split.
    * @param index         index of the returned partition.
    * @param numPartitions number of partitions.
    * @tparam T type of the list.
    * @return part of the list.
    */
  def splitList[T](input: JList[T], index: Int, numPartitions: Int): JList[T] = {
    val split = ArrayBuffer.empty[T]
    var i: Int = index
    while (i < input.size) {
      split.add(input.get(i))
      i += numPartitions
    }
    split
  }

}
