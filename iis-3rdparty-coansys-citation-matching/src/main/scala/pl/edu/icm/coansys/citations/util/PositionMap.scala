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

import collection.SortedMap

/**
 * There are some indices with specified position. Others position is defined in respect to adjacent defined indices.
 *
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class PositionMap(map: SortedMap[Int, Double]) {
  def position(i: Int) = {
    val (right, rv) = map.from(i).head
    val (left, lv) = map.to(i).last
    if (left == right)
      lv
    else
      lv + (i - left) * (rv - lv) / (right - left)
  }
}

object PositionMap {
  implicit def toPositionMap(map: SortedMap[Int, Double]) = new PositionMap(map)
}
