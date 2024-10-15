/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons

import annotation.tailrec
import util.matching.Regex
import scala.math.max

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
object strings {
  /**
   * Returns all rotations of a given string
   */
  def rotations(s: String): IndexedSeq[String] = {
    for {
      b <- 0 to (s.length - 1)
      rot = s.substring(b) + s.substring(0, b)
    } yield rot
  }

  /**
   * Computes HCS (Heaviest Common Subsequence) of two strings using given weight function.
   */
  def hcs(s1: String, s2: String, weight: Char => Double) = {
    val sndDim = s2.length + 1
    val hcsArray = Array.ofDim[Double]((s1.length + 1) * (s2.length + 1))
    for (i <- 0 to s1.length) {
      for (j <- 0 to s2.length)
        if (i == 0 || j == 0) {
          hcsArray(i * sndDim + j) = 0
        } else {
          val eqBonus = if (s1.charAt(i - 1) == s2.charAt(j - 1)) weight(s1.charAt(i - 1)) else 0
          hcsArray(i * sndDim + j) = List(hcsArray((i - 1) * sndDim + (j - 1)) + eqBonus, hcsArray((i - 1) * sndDim + j), hcsArray(i * sndDim + j - 1)).max
        }
    }

    @tailrec
    def extractHcs(i: Int, j: Int, acc: List[(Int, Int)] = Nil): List[(Int, Int)] = {
      if (i > 0 && j > 0) {
        if (hcsArray((i - 1) * sndDim + (j - 1)) >= max(hcsArray((i - 1) * sndDim + j), hcsArray(i * sndDim + (j - 1)))) {
          if (s1.charAt(i - 1) == s2.charAt(j - 1))
            extractHcs(i - 1, j - 1, (i - 1, j - 1) :: acc)
          else
            extractHcs(i - 1, j - 1, acc)
        } else {
          if (hcsArray((i - 1) * sndDim + j) >= hcsArray(i * sndDim + (j - 1)))
            extractHcs(i - 1, j, acc)
          else
            extractHcs(i, j - 1, acc)
        }
      }
      else {
        acc
      }
    }

    extractHcs(s1.length, s2.length)
  }

  /**
   * Computes LCS of two strings.
   */
  def lcs(s1: String, s2: String) =
    hcs(s1, s2, _ => 1)

  /**
   * Returns a subsequence of given string consisting of characters at specified positions.
   */
  def subsequence(s: String, indices: TraversableOnce[Int]) = {
    indices map (s.charAt) mkString ("")
  }

  /**
   * Splits a given strings on regex matches without removing matched text, e.g.
   * splitOnRegex(" ".r, "ala ma kota") == List("ala", " ", "ma", " ", "kota")
   */
  def splitOnRegex(regex: Regex, s: String): List[String] = {
    val groupBoundaries = (Iterator(0) ++ regex.findAllIn(s).matchData.flatMap {
      case m => List(m.start, m.end)
    } ++ Iterator(s.length)).toList
    val groupBoundariesPairs = groupBoundaries zip groupBoundaries.drop(1)
    groupBoundariesPairs flatMap {
      case (b, e) => if (b < e) Some(s.substring(b, e)) else None
    }
  }

  /**
   * All characters except letters replaced by spaces. Continuous whitespaces merged.
   */
  def lettersOnly(s: String) =
    s.map(x => if (x.isLetter) x else ' ').split("\\W+").mkString(" ")

  /**
   * All characters except digits replaced by spaces. Continuous whitespaces merged.
   */
  def digitsOnly(s: String) =
    s.map(x => if (x.isDigit) x else ' ').split("\\W+").mkString(" ")
}
