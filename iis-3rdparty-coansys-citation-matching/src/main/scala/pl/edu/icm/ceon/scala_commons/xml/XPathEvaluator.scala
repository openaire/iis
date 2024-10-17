/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons.xml

import java.io.{InputStream, StringReader}
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.{XPathConstants, XPathFactory}

import org.apache.commons.io.IOUtils
import org.w3c.dom.{Node, NodeList}
import org.xml.sax.{EntityResolver, InputSource}

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class XPathEvaluator(private val obj: Any) extends (String => String) {

  private val evaluator = XPathFactory.newInstance().newXPath()

  def apply(xpath: String): String =
    evaluator.evaluate(xpath, obj)

  def asNode(xpath: String) =
    evaluator.evaluate(xpath, obj, XPathConstants.NODE).asInstanceOf[Node]

  def asNodes(xpath: String) = {
    val nodes = evaluator.evaluate(xpath, obj, XPathConstants.NODESET).asInstanceOf[NodeList]
    for (i <- 0 until nodes.getLength)
    yield nodes.item(i)
  }

  def asStrings(xpath: String) =
    asNodes(xpath)
      .map(_.getTextContent)

}

object XPathEvaluator {

  def fromInputStream(is: InputStream) = {
    val docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder()
    docBuilder.setEntityResolver(new EntityResolver {
      def resolveEntity(publicId: String, systemId: String) = new InputSource(new StringReader(""))
    })
    val doc = docBuilder.parse(is)
    new XPathEvaluator(doc)
  }

  def fromString(s: String) = fromInputStream(IOUtils.toInputStream(s))

}