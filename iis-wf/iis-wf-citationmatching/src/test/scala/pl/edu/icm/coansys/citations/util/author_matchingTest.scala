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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import author_matching._

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class author_matchingTest {
  @Test
  def matchingTokensTest() {
    assertEquals(exactMatchingTokens("ala ma kota".split(" ").toList, "kot ma ale".split(" ").toList), List((1, 1)))
    assertEquals(exactMatchingTokens("ala ma kota".split(" ").toList, "kota ala ma".split(" ").toList), List((0, 1), (1, 2), (2, 0)))
  }

  @Test
  def roughMatchingTest() {
    assertEquals(nonOverlappingMatching(Set((0, 0), (1, 1))), List((0, 0), (1, 1)))
    assertEquals(nonOverlappingMatching(Set((0, 0), (1, 4), (2, 2), (3, 3), (5, 5))), List((0, 0), (2, 2), (3, 3), (5, 5)))
  }
}
