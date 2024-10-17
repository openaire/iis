/*
 * This file is part of CoAnSys project.
 * Copyright (c) 2012-2015 ICM-UW
 *
 * CoAnSys is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CoAnSys is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with CoAnSys. If not, see <http://www.gnu.org/licenses/>.
 */

package pl.edu.icm.coansys.citations.hashers

import pl.edu.icm.coansys.citations.data.MatchableEntity
import pl.edu.icm.coansys.citations.hashers.util._
import pl.edu.icm.coansys.citations.util.misc
import pl.edu.icm.coansys.citations.util.misc._
import scala.util.Try


/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class DocumentNameYearHashGenerator extends HashGenerator {
  override def generate(entity: MatchableEntity): Iterable[String] = for {
    author <- misc.lettersNormaliseTokenise(entity.author).filterNot(stopWords).distinct.take(4)
    year <- digitsNormaliseTokenise(entity.year).filter(_.length == 4).flatMap(x => Try(x.toInt).toOption).filter(x => x < 2050 && x > 1900)
    bluredYear <- blur(year)
  } yield List(author, bluredYear).mkString("#")
}
