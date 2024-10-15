/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons.classification.features

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class FeatureVectorBuilder[A](calculators: List[FeatureCalculator[A]]) {
  def calculateFeatureVectorValues(obj: A): Array[Double] =
    calculators.map(_.calculateValue(obj)).toArray
}
