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

import collection.immutable.SortedMap

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class Alignment(private val matches1: List[Int],
                private val matches2: List[Int],
                private val boundaries: List[Double]) {
  private val positionMap1 = new PositionMap(SortedMap((matches1 zip boundaries): _*))
  private val positionMap2 = new PositionMap(SortedMap((matches2 zip boundaries): _*))

  def distance(i: Int, j: Int) =
    math.abs(positionMap1.position(i + 1) - positionMap2.position(j + 1))
}
