/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons.collections

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DisjointSetTest {
  @Test
  def basicTest() {
    val set1 = new DisjointSet(1)
    val set2 = new DisjointSet(2)
    val set3 = new DisjointSet(3)
    val set4 = new DisjointSet(4)

    assertEquals(Set(1), set1.elements.toSet)
    assertEquals(Set(2), set2.elements.toSet)
    assertEquals(Set(3), set3.elements.toSet)
    assertEquals(Set(4), set4.elements.toSet)

    set1.union(set2)
    set4.union(set3)

    assertTrue(set1.find() == set2.find())
    assertTrue(set3.find() == set4.find())

    assertEquals(Set(1, 2), set1.elements.toSet)
    assertEquals(Set(1, 2), set2.elements.toSet)
    assertEquals(Set(3, 4), set3.elements.toSet)
    assertEquals(Set(3, 4), set4.elements.toSet)

    set1.union(set4)

    assertEquals(Set(1, 2, 3, 4), set1.elements.toSet)
    assertEquals(Set(1, 2, 3, 4), set2.elements.toSet)
    assertEquals(Set(1, 2, 3, 4), set3.elements.toSet)
    assertEquals(Set(1, 2, 3, 4), set4.elements.toSet)
  }
}