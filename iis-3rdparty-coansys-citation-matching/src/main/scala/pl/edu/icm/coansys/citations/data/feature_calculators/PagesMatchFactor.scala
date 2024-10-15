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
import pl.edu.icm.coansys.citations.util.misc
import pl.edu.icm.ceon.scala_commons.classification.features.FeatureCalculator

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
object PagesMatchFactor  extends FeatureCalculator[(MatchableEntity, MatchableEntity)] {
  def calculateValue(entities: (MatchableEntity, MatchableEntity)): Double = {
    val pages1 = misc.extractNumbers(entities._1.pages)
    val pages2 = misc.extractNumbers(entities._2.pages)
    if (pages1.size + pages2.size > 0)
      2 * (pages1.toSet & pages2.toSet).size.toDouble / (pages1.size + pages2.size)
    else
      0.0
  }
}
