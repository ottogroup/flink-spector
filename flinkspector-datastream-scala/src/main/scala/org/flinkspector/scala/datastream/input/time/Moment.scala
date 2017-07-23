package org.flinkspector.scala.datastream.input.time

import java.util.concurrent.TimeUnit

import org.flinkspector.datastream.input.time.{After => JAfter, Before => JBefore, InWindow => JInWindow}

import scala.concurrent.duration.Duration

object After {
  def apply(duration: Duration) : JAfter = {
    JAfter.period(duration.toNanos, TimeUnit.NANOSECONDS)
  }
}

object Before {
  def apply(duration: Duration) : JBefore = {
    JBefore.period(duration.toNanos, TimeUnit.NANOSECONDS)
  }
}

object InWindow {
  def apply(duration: Duration) : JInWindow = {
    JInWindow.to(duration.toNanos, TimeUnit.NANOSECONDS)
  }
}