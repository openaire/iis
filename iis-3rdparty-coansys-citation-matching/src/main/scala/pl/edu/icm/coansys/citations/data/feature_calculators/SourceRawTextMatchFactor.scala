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
import pl.edu.icm.ceon.scala_commons.strings
import pl.edu.icm.coansys.citations.util.misc._
import pl.edu.icm.ceon.scala_commons.classification.features.FeatureCalculator

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
object SourceRawTextMatchFactor  extends FeatureCalculator[(MatchableEntity, MatchableEntity)] {
  def calculateValue(entities: (MatchableEntity, MatchableEntity)): Double = {
    safeDiv(strings.lcs(entities._1.source, entities._2.rawText.getOrElse("")).length, entities._1.source.length) max
      safeDiv(strings.lcs(entities._2.source, entities._1.rawText.getOrElse("")).length, entities._2.source.length)
  }

}
