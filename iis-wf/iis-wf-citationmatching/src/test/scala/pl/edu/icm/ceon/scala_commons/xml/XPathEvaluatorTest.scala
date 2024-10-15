/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons.xml

import org.apache.commons.io.IOUtils
import org.w3c.dom.{Node, Element}
import org.jdom.input.DOMBuilder
import org.jdom.output.XMLOutputter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class XPathEvaluatorTest {
  @Test
  def someTest() {
    def nodeToString(n: Node) =
      new XMLOutputter().outputString(new DOMBuilder().build(n.asInstanceOf[Element]))
    val xmlString = """<article><back><ref-list><ref>test<b>test</b></ref></ref-list></back></article>"""
    val eval = XPathEvaluator.fromInputStream(IOUtils.toInputStream(xmlString))
    val result = eval.asNodes( """/article/back/ref-list/ref""")
      .map(nodeToString)
      .head
    assertEquals(result, """<ref>test<b>test</b></ref>""")
  }
}
