package com.hometask

import org.scalatest.{Matchers, WordSpec}
import com.hometask.Task1._

import scala.concurrent.duration._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.{Seconds, Span}

class Task1Test
  extends WordSpec
    with Matchers
    with Timeouts {


  "retry mechanism" should {

    "return calculated result on acceptable condition with out using retries attempts" in {
      failAfter(Span(2, Seconds)) {
        retry(
          () => 3,
          (e: Int) => e > 0,
          List(5.seconds)) shouldEqual 3
      }
    }

    "return calculated result on not acceptable condition by using retries attempts" in {
      val retries = List(1.seconds, 2.seconds, 3.seconds)
      val testStartedTime = System.currentTimeMillis()

      val actualResult = retry(
        () => 3,
        (e: Int) => e < 0,
        retries)

      val testFinishedTime = System.currentTimeMillis()
      val timeSpentOnTest = testFinishedTime - testStartedTime
      val timeSpentOnRetries = retries.map(_.toMillis).fold(0L)(_+_)

      actualResult shouldBe 3
      assert(timeSpentOnTest > timeSpentOnRetries)
    }

    "return calculated result on acceptable condition with empty retries" in {
      retry(
        () => 3,
        (e: Int) => e > 0,
        List.empty) shouldEqual 3
    }

    "return calculated result on not acceptable condition with empty retries" in {
      retry(
        () => 3,
        (e: Int) => e < 0,
        List.empty) shouldEqual 3
    }
  }
}
