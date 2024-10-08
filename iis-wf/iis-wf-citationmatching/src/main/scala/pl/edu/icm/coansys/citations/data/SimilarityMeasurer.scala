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

import feature_calculators._
import pl.edu.icm.ceon.scala_commons.classification.features.FeatureVectorBuilder
import pl.edu.icm.ceon.scala_commons.classification.svm.SvmClassifier

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class SimilarityMeasurer(val featureVectorBuilder:FeatureVectorBuilder[(MatchableEntity, MatchableEntity)] =
                           SimilarityMeasurer.advancedFvBuilder) {
  val classifier = SvmClassifier.fromResource("/pl/edu/icm/coansys/citations/coraHeurBal.model")

  def similarity(e1: MatchableEntity, e2: MatchableEntity): Double =
    classifier.predictProbabilities(featureVectorBuilder.calculateFeatureVectorValues((e1, e2)))(1)
}

object SimilarityMeasurer {
  val simpleFvBuilder = new FeatureVectorBuilder(List(
    AuthorTrigramMatchFactor,
    AuthorTokenMatchFactor,
    PagesMatchFactor,
    SourceMatchFactor,
    TitleMatchFactor,
    YearMatchFactor))

  val advancedFvBuilder = new FeatureVectorBuilder(List(
    AuthorMatchFactor,
    AuthorTrigramMatchFactor,
    AuthorTokenMatchFactor,
    PagesMatchFactor,
    PagesRawTextMatchFactor,
    SourceMatchFactor,
    SourceRawTextMatchFactor,
    TitleMatchFactor,
    TitleTokenMatchFactor,
    YearMatchFactor,
    YearRawTextMatchFactor))

  def main(args: Array[String]) {
    val measurer = new SimilarityMeasurer
    val doc1 = MatchableEntity.fromParameters("1", "Jan Kowalski", "J. App. Phis.", "Some random title", "120-126", "2010")
    val doc2 = MatchableEntity.fromParameters("2", "Jan Kowalski", "J. App. Phis.", "Totally different title", "32-36", "2010")
    val doc3 = MatchableEntity.fromParameters("3", "Zbigniew Nowak", "Przegląd leśniczy", "Inny tytuł", "15-20", "1995")
    println(measurer.similarity(doc1, doc1))
    println(measurer.similarity(doc1, doc2))
    println(measurer.similarity(doc1, doc3))

  }
}
