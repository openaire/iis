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
import pl.edu.icm.coansys.citations.util.misc.normalizedAuthorTokensFromAuthorList
import pl.edu.icm.coansys.models.DocumentProtos.{BasicMetadata, Author}

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class normalizedAuthorTokensFromAuthorListTest {
  @Test
  def noAuthorsTest() {
    val meta = BasicMetadata.newBuilder().build()
    assertEquals(normalizedAuthorTokensFromAuthorList(meta), Set())
  }

  @Test
  def emptyAuthorTest() {
    val author = Author.newBuilder().setKey("1").build()
    val meta = BasicMetadata.newBuilder().addAuthor(author).build()
    assertEquals(author.getName, "")
    assertEquals(normalizedAuthorTokensFromAuthorList(meta), Set())
  }

  @Test
  def authorWithSomeDataTest() {
    val author = Author.newBuilder().setKey("1").setName("Kowalski").build()
    val meta = BasicMetadata.newBuilder().addAuthor(author).build()
    assertEquals(normalizedAuthorTokensFromAuthorList(meta), Set("kowalski"))
  }

  @Test
  def specialCharsInNameTest() {
    val author = Author.newBuilder().setKey("1").setName("Kowalski, Jan").build()
    val meta = BasicMetadata.newBuilder().addAuthor(author).build()
    assertEquals(normalizedAuthorTokensFromAuthorList(meta), Set("kowalski", "jan"))
  }

  @Test
  def abbreviationsTest() {
    val author1 = Author.newBuilder().setKey("1").setName("Qi Lu").build()
    val author2 = Author.newBuilder().setKey("1").setName("J. Kowalski").build()
    val meta = BasicMetadata.newBuilder().addAuthor(author1).addAuthor(author2).build()
    assertEquals(normalizedAuthorTokensFromAuthorList(meta), Set("kowalski", "qi", "lu"))
  }
}
