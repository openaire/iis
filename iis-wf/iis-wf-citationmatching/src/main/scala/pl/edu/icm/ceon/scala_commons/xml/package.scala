/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons

import org.w3c.dom.Node

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
package object xml {

  sealed trait Elem

  case class Text(s: String) extends Elem

  case class StartTag(name: String) extends Elem

  case class EndTag(name: String) extends Elem

  private val tagRegex = "(<[^>]+>)".r

  /**
   * Converts XML string to Elem list.
   */
  def xmlToElems(xml: String): List[Elem] = {
    def tagContentToElem(s: String): Elem =
      if (s.charAt(0) != '/') StartTag(s) else EndTag(s.substring(1))

    val groupBoundaries = tagRegex.findAllIn(xml).matchData map {
      m => (m.start, m.end)
    }
    val (part, end) = groupBoundaries.foldLeft((Nil: List[Elem], 0)) {
      case ((tail, begin), (b, e)) =>
        val content = xml.substring(b + 1, e - 1)
        val list: List[Elem] =
          if (begin < b)
            tagContentToElem(content) :: Text(xml.substring(begin, b)) :: tail
          else
            tagContentToElem(content) :: tail
        (list, e)
    }
    val reversed =
      if (end < xml.length)
        Text(xml.substring(end)) :: part
      else
        part
    reversed.reverse
  }

  //  def removeTags(s: String):String =
  //    removeTags(s, "")

  def removeTags(s: String, joiner: String = ""): String = {
    val elems = strings.splitOnRegex(tagRegex, s)
    val removedTags = elems filterNot (tagRegex.pattern.matcher(_).matches)
    removedTags.mkString(joiner)
  }

  def tagset(n: Node): Set[String] =
    if (n.getNodeType != Node.ELEMENT_NODE) {
      Set()
    } else {
      val children = n.getChildNodes
      val childTags = (0 until children.getLength).map(children.item).map(tagset).fold(Set())(_ ++ _)
      childTags + n.getNodeName
    }
}
