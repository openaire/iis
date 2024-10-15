/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import strings._

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class strigsTest {
  @Test
  def rotationsTest() {
    assertEquals(Set("abc", "bca", "cab"), rotations("abc").toSet)
  }

  @Test
  def subsequenceTest() {
    assertEquals("", subsequence("abcdef", Seq()))
    assertEquals("adf", subsequence("abcdef", Seq(0, 3, 5)))
  }

  @Test
  def lcsTest() {
    assertEquals(Seq((0, 0), (1, 1), (2, 2)), lcs("aaa", "aaa"))
    assertEquals("aa", subsequence("aba", lcs("aba", "aaa").unzip._1))
    assertEquals("MJAU", subsequence("XMJYAUZ", lcs("XMJYAUZ", "MZJAWXU").unzip._1))
    assertEquals("MJAU", subsequence("MZJAWXU", lcs("XMJYAUZ", "MZJAWXU").unzip._2))
    assertEquals("HMAN", subsequence("HUMAN", lcs("HUMAN", "CHIMPANZEE").unzip._1))
    assertEquals("HMAN", subsequence("CHIMPANZEE", lcs("HUMAN", "CHIMPANZEE").unzip._2))
  }

  @Test
  def hcsTest() {
    def defaultWeight(x: Char): Double = 1.0
    assertEquals(Seq((0, 0), (1, 1), (2, 2)), hcs("aaa", "aaa", defaultWeight))
    assertEquals("aa", subsequence("aba", hcs("aba", "aaa", defaultWeight).unzip._1))
    assertEquals("MJAU", subsequence("XMJYAUZ", hcs("XMJYAUZ", "MZJAWXU", defaultWeight).unzip._1))
    assertEquals("MJAU", subsequence("MZJAWXU", hcs("XMJYAUZ", "MZJAWXU", defaultWeight).unzip._2))
    assertEquals("HMAN", subsequence("HUMAN", hcs("HUMAN", "CHIMPANZEE", defaultWeight).unzip._1))
    assertEquals("HMAN", subsequence("CHIMPANZEE", hcs("HUMAN", "CHIMPANZEE", defaultWeight).unzip._2))

    def avoidSpaceWeight(x: Char): Double = {
      val epsilon = 0.0001
      x match {
        case ' ' => 1.0 - epsilon
        case _ => 1.0
      }
    }

    assertEquals("abc", subsequence("a bc", hcs("a bc", "ab c", avoidSpaceWeight).unzip._1))
    assertEquals("abc", subsequence("ab c", hcs("a bc", "ab c", avoidSpaceWeight).unzip._2))
    assertEquals("abc", subsequence("ab c", hcs("ab c", "a bc", avoidSpaceWeight).unzip._1))
    assertEquals("abc", subsequence("a bc", hcs("ab c", "a bc", avoidSpaceWeight).unzip._2))
  }

  @Test
  def splitOnRegexTest() {
    assertEquals(splitOnRegex(" ".r, "ala ma kota").length, 5)
    assertEquals(splitOnRegex(" ".r, "ala ma kota").mkString(""), "ala ma kota")
    assertEquals(splitOnRegex(" ".r, "ala  ma  kota").length, 7)
    assertEquals(splitOnRegex(" ".r, "ala  ma  kota").mkString(""), "ala  ma  kota")
  }
}
