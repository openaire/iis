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
import misc._
import scala.util.Random

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class miscTest {
  @Test
  def uuidEncoderDecoderEmptyTest() {
    val s = ""
    assertEquals(misc.uuidDecode(misc.uuidEncode(s)), s)
  }

  @Test
  def uuidEncoderDecoderNonEmptyTest() {
    val s = "ala ma kota"
    assertEquals(misc.uuidDecode(misc.uuidEncode(s)), s)
  }

  @Test
  def uuidEncoderDecoderUtf8Test() {
    val s = "zażółć gęślą jaźń"
    assertEquals(misc.uuidDecode(misc.uuidEncode(s)), s)
  }

  @Test
  def extractNumbersTest() {
    assertEquals(extractNumbers(""), Nil, "Empty string")
    assertEquals(extractNumbers("320"), List("320"), "One number")
    assertEquals(extractNumbers("1999r."), List("1999"), "Number with some chars afterwards")
    assertEquals(extractNumbers("14 1992"), List("14", "1992"), "Two numbers")
    assertEquals(extractNumbers("10-11"), List("10", "11"), "Two numbers separated by dash")
    assertEquals(extractNumbers("pp.10-11."), List("10", "11"), "Two numbers separated by dash and some rubbish")
  }

  @Test
  def extractYearTest() {
    assertEquals(extractYear(""), None, "Empty string")
    assertEquals(extractYear("320"), None, "Too short numeral")
    assertEquals(extractYear("1999"), Some("1999"), "Only a year")
    assertEquals(extractYear("1999r."), Some("1999"), "Year with some chars afterwards")
    assertEquals(extractYear("19992"), None, "Too long to be year")
    assertEquals(extractYear("14 1992"), Some("1992"), "Two numerals, only one is year")
    assertEquals(extractYear("9999 1992"), Some("1992"), "Two four-digit numerals, one is year")
    assertEquals(extractYear("1000 1992"), Some("1992"), "Two four-digit numerals, one more likely to be year")
  }

  @Test
  def nGreatestTest() {
    assertEquals(nGreatest(1 to 200, 100).toSet, (101 to 200).toSet)
    assertEquals(nGreatest(Random.shuffle((1 to 200).toList), 100).toSet, (101 to 200).toSet)
    assertEquals(nGreatest(1 to 10, 100).toSet, (1 to 10).toSet)
  }
}
