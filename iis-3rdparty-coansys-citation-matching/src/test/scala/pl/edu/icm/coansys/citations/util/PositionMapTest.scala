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
import collection.immutable.SortedMap

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class PositionMapTest {
  val epsilon = 0.000001

  @Test
  def simpleTest() {
    val map = new PositionMap(SortedMap(1 -> 0.5, 3 -> 1.0))
    assertEquals(map.position(1), 0.5, epsilon)
    assertEquals(map.position(3), 1, epsilon)
    assertEquals(map.position(2), 0.75, epsilon)
  }

  @Test
  def biggerIntervalTest() {
    val map = new PositionMap(SortedMap(5 -> 0.5, 10 -> 1.0))
    assertEquals(map.position(5), 0.5, epsilon)
    assertEquals(map.position(6), 0.6, epsilon)
    assertEquals(map.position(7), 0.7, epsilon)
    assertEquals(map.position(8), 0.8, epsilon)
    assertEquals(map.position(9), 0.9, epsilon)
    assertEquals(map.position(10), 1.0, epsilon)
  }

  @Test
  def moreIntervalsTest() {
    val map = new PositionMap(SortedMap(3 -> 0.3, 5 -> 0.5, 10 -> 1.0))
    assertEquals(map.position(3), 0.3, epsilon)
    assertEquals(map.position(4), 0.4, epsilon)
    assertEquals(map.position(5), 0.5, epsilon)
    assertEquals(map.position(6), 0.6, epsilon)
    assertEquals(map.position(7), 0.7, epsilon)
    assertEquals(map.position(8), 0.8, epsilon)
    assertEquals(map.position(9), 0.9, epsilon)
    assertEquals(map.position(10), 1.0, epsilon)
  }
}
