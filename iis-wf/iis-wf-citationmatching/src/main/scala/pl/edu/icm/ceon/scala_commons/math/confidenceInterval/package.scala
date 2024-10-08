package pl.edu.icm.ceon.scala_commons.math

import org.apache.commons.math.distribution.TDistributionImpl
import org.apache.commons.math.stat.descriptive.SummaryStatistics
import Math.sqrt

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 *         Created: 29.08.2013 11:36
 */
package object confidenceInterval {
  type Interval = (Double, Double)

  def getConfidenceInterval(stats: SummaryStatistics, confidenceLevel: Double): Interval = {
    val avg = stats.getMean
    val error = getConfidenceIntervalError(stats, confidenceLevel)

    (avg - error, avg + error)
  }

  def getConfidenceIntervalError(stats: SummaryStatistics, confidenceLevel: Double): Double = {
    val sd = stats.getStandardDeviation
    val n = stats.getN
    val alpha = 1 - confidenceLevel

    val tDist = new TDistributionImpl(stats.getN - 1)
    val qt = tDist.inverseCumulativeProbability(1 - alpha / 2)

    qt * sd / sqrt(n)
  }
}
