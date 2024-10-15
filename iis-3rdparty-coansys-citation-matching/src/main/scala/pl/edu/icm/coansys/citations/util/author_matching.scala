/*
 * This file is part of CoAnSys project.
 * Copyright (c) 2012-2015 ICM-UW
 * 
 * CoAnSys is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * CoAnSys is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with CoAnSys. If not, see <http://www.gnu.org/licenses/>.
 */

package pl.edu.icm.coansys.citations.util

import org.apache.commons.lang.StringUtils
import pl.edu.icm.ceon.scala_commons.collections
import annotation.tailrec
import pl.edu.icm.ceon.scala_commons.math.SigmoidFunction

/**
 * An object supplying a fancy author matching method, v. arXiv:1303.6906 [cs.IR].
 *
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
object author_matching {
  def nonOverlappingMatching(exactMatches: Set[(Int, Int)]) = {
    val len1 = exactMatches.toIterable.unzip._1.max
    val len2 = exactMatches.toIterable.unzip._2.max
    val roughMatchingVal = collections.tabulate[Int](len1 + 1, len2 + 1) {
      (a, i, j) =>
        if (i == 0 || j == 0)
          0
        else
        if (exactMatches((i - 1, j - 1)))
          a(i - 1)(j - 1) + 1
        else
          math.max(a(i - 1)(j), a(i)(j - 1))
    }

    @tailrec
    def extract(i: Int, j: Int, acc: List[(Int, Int)] = Nil): List[(Int, Int)] = {
      if (i > 0 && j > 0) {
        if (exactMatches((i - 1, j - 1))) {
          extract(i - 1, j - 1, (i - 1, j - 1) :: acc)
        } else {
          if (roughMatchingVal(i - 1)(j) >= roughMatchingVal(i)(j - 1))
            extract(i - 1, j, acc)
          else
            extract(i, j - 1, acc)
        }
      }
      else {
        acc
      }
    }

    extract(len1 + 1, len2 + 1)
  }

  /**
   * Returns a list of matching tokens indices. Exact match is used.
   *
   * @return pairs of matching tokens indices
   */
  def exactMatchingTokens(tokens1: List[String], tokens2: List[String]) = for {
    (w, i) <- tokens1.zipWithIndex
    (v, j) <- tokens2.zipWithIndex
    if w == v
  } yield (i, j)


  def matchFactor(tokens1: List[String], tokens2: List[String]) =
    if (!tokens1.isEmpty && !tokens2.isEmpty)
      matchFactorOnNonEmpty(tokens1, tokens2)
    else
    if (tokens1 == tokens2) 1.0 else 0.0

  def matchFactorOnNonEmpty(tokens1: List[String], tokens2: List[String]) = {
    val limit = 500
    val trimmedTokens1 = tokens1.take(limit)
    val trimmedTokens2 = tokens2.take(limit)
    val len1 = trimmedTokens1.length
    val len2 = trimmedTokens2.length

    //Add begin and end tokens (they always match)
    val extendedExactMatchingTokens =
      exactMatchingTokens(trimmedTokens1, trimmedTokens2).map {
        case (x, y) => (x + 1, y + 1)
      }.toSet + ((0, 0)) + ((len1 + 1, len2 + 1))

    // Its a matching whose match edges don't overlap
    val nonOverlapping = nonOverlappingMatching(extendedExactMatchingTokens)

    // Average positions of matching tokens
    val boundaries = nonOverlapping.map {
      case (i, j) => (i.toDouble / (len1 + 1) + j.toDouble / (len2 + 1)) / 2
    }

    val alignment = new Alignment(nonOverlapping.unzip._1, nonOverlapping.unzip._2, boundaries)

    val dists = Array.tabulate(len1, len2)(alignment.distance)

    val importance1 = Array.tabulate(len1) {
      i => val v = trimmedTokens1(i).length.toDouble / trimmedTokens1.mkString("").length; v
    }
    val importance2 = Array.tabulate(len2) {
      i => val v = trimmedTokens2(i).length.toDouble / trimmedTokens2.mkString("").length; v
    }

    val editDists = Array.tabulate(len1, len2) {
      (i, j) =>
        if (trimmedTokens1(i) == trimmedTokens2(j))
          0
        else if (((trimmedTokens1(i) startsWith trimmedTokens2(j)) && (trimmedTokens2(j).length <= 2)) ||
          ((trimmedTokens2(j) startsWith trimmedTokens1(i)) && (trimmedTokens1(i).length <= 2)))
          1
        else
          StringUtils.getLevenshteinDistance(trimmedTokens1(i), trimmedTokens2(j))
    }

    val maxLen = math.max(len1, len2)
    val distTransform: Double => Double = SigmoidFunction.genBetter(0.01, 6.0 / ((len1 + len2) / 2.0))
    val editDistTransform: Int => Double = ((i: Int) => i.toDouble) andThen SigmoidFunction.genBetter(0.01, 4)
    val weights = Array.tabulate(maxLen, maxLen) {
      case (i, j) if i < len1 && j < len2 =>
        distTransform(dists(i)(j)) * editDistTransform(editDists(i)(j)) * (importance1(i) + importance2(j))
      case _ => 0.0
    }
    val maxWeight = weights.flatten.foldLeft(0.0)(_ max _)
    val aleredWeights = Array.tabulate(maxLen, maxLen) {
      (i, j) => maxWeight - weights(i)(j)
    }

    val matching = new HungarianAlgorithm(aleredWeights).execute()

    (matching.zipWithIndex.map {
      case (j, i) => weights(i)(j)
    }.sum) / (importance1.sum + importance2.sum)
  }

  def main(args: Array[String]) {
    val cases = List(
      ("ala ma kota", "kot ma ale"),
      ("Jan Kowalski", "Jan Kowalski"),
      ("J Kowalski", "Jan Kowalski"),
      ("Kowalski", "Jan Kowalski"),
      ("Jan Kowalski", "Kowalski Jan"),
      ("Jan Kowalski", "Józef Kowalski"),
      ("Jan Kowalski", "J Kowalski"),
      ("Jan Kowalski", "Kowalski J"),
      ("Jan Kowalski", "Jan A Kowalski"),
      ("Jan Kowalski", "Kowalski J A"),
      ("Jan Kowalski", "Jan A Kowalski"),
      ("Andrzej Nowak", "Kowalski J A"),
      ("Jan Kowalski Andrzej Nowak", "Kowalski J A Nowak A S"),
      ("Jan Kowalski Andrzej Nowak Krzysztof Tyszkiewicz", "Kowalski J A Nowak A S Tyszkiewicz K J"),
      ("Jan Kowalski Andrzej Nowak Krzysztof Tyszkiewicz", "Kowalski J A Nowak A S Tyszkiewicz K J Banasiak M"),
      ("Jan Kowalski Andrzej Nowak Krzysztof Tyszkiewicz", "Kowalski J A Banasiak M Nowak A S Tyszkiewicz K J"),
      ("Jan Kowalski Andrzej Nowak", "Jan A Kowalski"),
      ("Jan Kowalski Andrzej Kowalski", "Jan A Kowalski"),
      ("Jan Kowalski Andrzej Nowak", "Jan W Kowalski"),
      ("Jan Kowalski Andrzej Kowalski", "Jan W Kowalski"),
      ("Jan Kowalski Andrzej Nowak", "Andrzej Nowak Jan Kowalski"))
    for ((s1, s2) <- cases) {
      print("---%s--- ---%s--- ".format(s1, s2))
      println(matchFactor(s1.split(" ").toList, s2.split(" ").toList))
    }
  }
}
