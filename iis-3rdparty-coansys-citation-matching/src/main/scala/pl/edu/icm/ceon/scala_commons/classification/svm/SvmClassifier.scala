/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons.classification.svm

import java.io._
import libsvm.{svm, svm_node, svm_model}


/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class SvmClassifier(private val model: svm_model) {
  def predictProbabilities(fv: Array[Double]): Map[Int, Double] = {
    val instance: Array[svm_node] =
      fv.zipWithIndex.map{ case (value, index) =>
        val node = new svm_node()
        node.index = index + 1
        node.value = value
        node
      }
    val probEstimates: Array[Double] = new Array[Double](model.label.length)
    svm.svm_predict_probability(model, instance, probEstimates)
    (model.label zip probEstimates).toMap
  }
}

object SvmClassifier {
  def fromFile(file: File) =
    fromReader(new FileReader(file))

  def fromResource(resource: String) =
    fromReader(new InputStreamReader(getClass.getResourceAsStream(resource)))

  def fromReader(reader: Reader) =
    new SvmClassifier(svm.svm_load_model(new BufferedReader(reader)))

  /**
   * Converts a feature vector to the text line in LibSVM format.
   */
  def featureVectorValuesToLibSvmLine(fv: Iterable[Double], label: Int): String = {
    val features = (Stream.from(1) zip fv).map {
      case (i, v) => i + ":" + v
    }.mkString(" ")
    label + " " + features
  }
}
