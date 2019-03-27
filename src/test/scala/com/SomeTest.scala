package com

import org.scalatest.{FreeSpec, Matchers}

class SomeTest extends FreeSpec with Matchers{
  "first test should add two numbers correctly" in {
    Calculator.add(1, 1) shouldEqual 2
  }
}
