/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons.xml

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.apache.commons.io.IOUtils

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class packageTest {
  @Test
  def xmlToElemsTest() {
    assertEquals(xmlToElems("<tag>"), List(StartTag("tag")))
    assertEquals(xmlToElems("<tag>text</tag>"), List(StartTag("tag"), Text("text"), EndTag("tag")))
    assertEquals(xmlToElems("begin<tag>text</tag>end"), List(Text("begin"), StartTag("tag"), Text("text"), EndTag("tag"), Text("end")))
  }

  @Test
  def tagsetTest() {
    val eval = XPathEvaluator.fromInputStream(IOUtils.toInputStream("""<a>text<b>text<c sth="sth"/></b>text</a>"""))
    assertEquals(Set("b", "c"), tagset(eval.asNode("/a/b")))
    assertEquals(Set("a", "b", "c"), tagset(eval.asNode("/a")))
  }
}
