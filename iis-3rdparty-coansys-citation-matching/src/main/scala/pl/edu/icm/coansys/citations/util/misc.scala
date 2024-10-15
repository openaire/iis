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

import pl.edu.icm.coansys.models.DocumentProtos.{DocumentWrapper, ReferenceMetadata, BasicMetadata}
import scala.collection.JavaConversions._
import pl.edu.icm.coansys.citations.data.MatchableEntity
import pl.edu.icm.cermine.bibref.parsing.tools.CitationUtils
import pl.edu.icm.cermine.bibref.CRFBibReferenceParser
import pl.edu.icm.ceon.scala_commons.strings
import pl.edu.icm.coansys.commons.java.DiacriticsRemover
import scala.collection.mutable
import scala.util.Try
import java.util.Locale

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
object misc {
  def normalizedAuthorTokensFromAuthorList(meta: BasicMetadata) = {
    meta.getAuthorList.toIterable
      .flatMap {
      author =>
        List(
          author.getName,
          author.getForenames,
          author.getSurname).flatMap(_.split( """[^\p{L}]+"""))
    }
      .filter(_.length > 1)
      .map(_.toLowerCase)
      .toSet
  }

  def lettersNormaliseTokenise(str: String) =
    normaliseTokenise(strings.lettersOnly(str))

  def digitsNormaliseTokenise(str: String) =
    strings.digitsOnly(str).split(" ").toList

  def normaliseTokenise(str: String) =
    tokensFromCermine(DiacriticsRemover.removeDiacritics(str))
      .flatMap {
      tok =>
        if (tok.length <= 3 && tok.forall(_.isUpper))
          None
        else
          Some(tok)
    }
      .filter(_.length > 2)
      .map(_.toLowerCase)

  private val uuidCharset = "UTF-8"

  def uuidEncode(uuid: String): Array[Byte] =
    uuid.getBytes(uuidCharset)

  def uuidDecode(bytes: Array[Byte]): String =
    new String(bytes, uuidCharset)

  def extractNumbers(s: String): List[String] = {
    """(?<=(^|\D))\d+(?=($|\D))""".r.findAllIn(s).toList
  }

  def extractYear(s: String): Option[String] = {
    val baseYear = 2000
    val candidates = extractNumbers(s).filter(_.length == 4).map(_.toInt)

    if (candidates.isEmpty)
      None
    else
      Some(candidates.minBy(x => math.abs(x - baseYear)).toString)

  }

  def safeDiv(i1: Int, i2: Int, alternative: => Double = 0.0): Double =
    if (i2 == 0)
      alternative
    else
      i1.toDouble / i2

  def tokensFromCermine(s: String): List[String] =
    CitationUtils.stringToCitation(s).getTokens.map(_.getText).toList

  def niceTokens(s: String) =
    tokensFromCermine(s.toLowerCase(Locale.ENGLISH)).filter(x => x.length > 2 || x.exists(_.isDigit)).take(50).toSet

  def nGreatest[A : Ordering](elems: TraversableOnce[A], n: Int): Seq[A] = {
    var q = mutable.SortedSet()
    for (elem <- elems) {
      q.add(elem)
      if (q.size > n)
        q.remove(q.min)
    }

    q.toSeq
  }

  def approximateYear(year: String) = for {
    diff <- -1 to 1
    year <- Try(year.toInt).toOption
  } yield (year + diff).toString

  val stopWords =
    """
      |a
      |about
      |above
      |after
      |again
      |against
      |already
      |also
      |although
      |always
      |among
      |an
      |and
      |another
      |any
      |anyone
      |are
      |as
      |at
      |avec
      |be
      |been
      |before
      |being
      |but
      |by
      |can
      |cannot
      |could
      |d
      |das
      |de
      |dem
      |der
      |des
      |did
      |die
      |do
      |does
      |doing
      |done
      |du
      |due
      |durch
      |ein
      |eine
      |einer
      |eines
      |either
      |else
      |esp
      |etc
      |ever
      |for
      |from
      |further
      |gave
      |get
      |gets
      |give
      |given
      |gives
      |giving
      |got
      |had
      |has
      |have
      |having
      |hence
      |here
      |how
      |however
      |if
      |in
      |instead
      |into
      |is
      |it
      |its
      |itself
      |just
      |la
      |le
      |les
      |made
      |make
      |may
      |might
      |mit
      |more
      |most
      |mostly
      |much
      |must
      |nach
      |neither
      |none
      |nor
      |not
      |now
      |of
      |often
      |on
      |only
      |or
      |other
      |ought
      |our
      |out
      |par
      |same
      |see
      |seen
      |so
      |some
      |sometime
      |somewhat
      |such
      |sur
      |take
      |takes
      |taking
      |than
      |that
      |the
      |their
      |theirs
      |them
      |then
      |there
      |therefore
      |these
      |they
      |this
      |those
      |though
      |through
      |throughout
      |to
      |too
      |took
      |ueber
      |um
      |un
      |une
      |until
      |up
      |upon
      |use
      |used
      |using
      |vol
      |vom
      |von
      |vor
      |was
      |were
      |what
      |when
      |where
      |whether
      |which
      |while
      |who
      |whose
      |why
      |will
      |with
      |within
      |without
      |would
      |yet
      |zu
      |zum
      |zur
    """.stripMargin.split(raw"\s+").toSet
}
