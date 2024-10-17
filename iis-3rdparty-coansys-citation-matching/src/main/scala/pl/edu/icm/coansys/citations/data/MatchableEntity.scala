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

package pl.edu.icm.coansys.citations.data

import collection.JavaConversions._

import org.apache.commons.lang.StringUtils

import pl.edu.icm.coansys.citations.util.misc
import pl.edu.icm.coansys.citations.data.CitationMatchingProtos.MatchableEntityData;
import pl.edu.icm.coansys.commons.java.DiacriticsRemover.removeDiacritics

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class MatchableEntity(val data: MatchableEntityData) extends Serializable {
  def id = data.getId

  def author = removeDiacritics(data.getAuthor)

  def source = removeDiacritics(data.getSource)

  def title = removeDiacritics(data.getTitle)

  def pages = data.getPages

  def year = data.getYear

  def issue = data.getIssue

  def volume = data.getVolume

  def rawText =
    if (data.hasRawText)
      Some(removeDiacritics(data.getRawText))
    else
      data.getAuxiliaryList.find(_.getKey == "rawText").map(x => removeDiacritics(x.getValue))


  def normalisedAuthorTokens: Iterable[String] =
    misc.lettersNormaliseTokenise(author).distinct

  def toReferenceString: String =
    rawText.getOrElse(List(author, title, source, issue, volume, pages, year).mkString("; "))

  def toDebugString: String =
    s"id: $id\n" +
      s"author: $author\n" +
      s"author tokens: $normalisedAuthorTokens\n" +
      s"source: $source\n" +
      s"title: $title\n" +
      s"pages: $pages\n" +
      s"year: $year\n" +
      s"issue: $issue\n" +
      s"raw text: $rawText\n"

  override def hashCode = id.hashCode

  override def equals(other: Any) : Boolean = other match {
    case that: MatchableEntity => that.id == this.id
    case _ => false
  }
}

object MatchableEntity {

  def fromBytes(bytes: Array[Byte]): MatchableEntity = {
    new MatchableEntity(MatchableEntityData.parseFrom(bytes))
  }

  def fromParameters(id: String,
                     author: String = "",
                     source: String = "",
                     title: String = "",
                     pages: String = "",
                     year: String = "",
                     rawText: String = ""): MatchableEntity =
    fromParametersExt(id = id, author = author, source = source, title = title, pages = pages, year = year, rawText = rawText)


  def fromParametersExt(id: String,
                     author: String = "",
                     source: String = "",
                     title: String = "",
                     pages: String = "",
                     year: String = "",
                     issue: String = "",
                     volume: String = "",
                     rawText: String = ""): MatchableEntity = {
    val data = MatchableEntityData.newBuilder()
    data.setId(id)
    if (StringUtils.isNotBlank(author))
      data.setAuthor(author)
    if (StringUtils.isNotBlank(source))
      data.setSource(source)
    if (StringUtils.isNotBlank(title))
      data.setTitle(title)
    if (StringUtils.isNotBlank(pages))
      data.setPages(pages)
    if (StringUtils.isNotBlank(year))
      data.setYear(year)
    if (StringUtils.isNotBlank(issue))
      data.setIssue(issue)
    if (StringUtils.isNotBlank(volume))
      data.setVolume(volume)
    if (StringUtils.isNotBlank(rawText))
      data.setRawText(rawText)

    new MatchableEntity(data.build())
  }


}
