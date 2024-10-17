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

package pl.edu.icm.coansys.citations.data.feature_calculators

import pl.edu.icm.coansys.citations.data.MatchableEntity
import java.util.Locale
import pl.edu.icm.coansys.citations.util.misc._
import pl.edu.icm.ceon.scala_commons.classification.features.FeatureCalculator

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
object TitleTokenMatchFactor extends FeatureCalculator[(MatchableEntity, MatchableEntity)] {
  def calculateValue(entities: (MatchableEntity, MatchableEntity)): Double = {
    val tokens1 = tokensFromCermine(entities._1.title.toLowerCase(Locale.ENGLISH))
    val tokens2 = tokensFromCermine(entities._2.title.toLowerCase(Locale.ENGLISH))
    val counts1 = tokens1.map(_.toLowerCase).groupBy(identity).mapValues(_.length)
    val counts2 = tokens2.map(_.toLowerCase).groupBy(identity).mapValues(_.length)
    val common = (counts1.keySet & counts2.keySet).toIterator.map(k => counts1(k) min counts2(k)).sum

    if ((tokens1.length min tokens2.length) > 0)
      common.toDouble / (tokens1.length min tokens2.length)
    else
      0.0
  }
}
