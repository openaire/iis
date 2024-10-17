/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons.math

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test


/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class SigmoidFunctionTest {
  @Test
  def genTest() {
    val threshold = List(0.05, 0.001)
    val span = List(10.0, 40.0)
    for (
      th <- threshold;
      s <- span
    ) {
      val f = SigmoidFunction.gen(th, s)
      assertEquals(f(0), 1 - th, 0.000001)
      assertEquals(f(s), th, 0.000001)
    }
  }

  @Test
  def genBetterTest() {
    val threshold = List(0.05, 0.001)
    val span = List(10.0, 40.0)
    for (
      th <- threshold;
      s <- span
    ) {
      val f = SigmoidFunction.genBetter(th, s)
      assertEquals(f(0), 1, 0.000001)
      assertEquals(f(s), th, 0.000001)
    }


  }
}
