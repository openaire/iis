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

/**
 * Helper functions and classes for comparing string similarity.
 *
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
object ngrams {

  case class NgramStatistics(counter: Map[String, Int], overall: Int) {
    def similarityTo(other: NgramStatistics): Double = {
      val c1 = counter
      val c2 = other.counter
      val all = overall + other.overall
      val common = (c1.keySet & c2.keySet).toIterator.map(k => c1(k) min c2(k)).sum

      if (all > 0)
        2 * common.toDouble / all
      else
        0.0
    }
  }

  object NgramStatistics {
    def fromString(s: String, n: Int): NgramStatistics = {
      val counter = s.sliding(n).toIterable.groupBy(identity).mapValues(_.size)
      val overall = counter.values.sum
      NgramStatistics(counter, overall)
    }
  }

  def trigramSimilarity(s1: String, s2: String): Double = {
    val n = 3
    NgramStatistics.fromString(s1, n) similarityTo NgramStatistics.fromString(s2, n)
  }
}
