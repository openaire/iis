package pl.edu.icm.ceon.scala_commons.math

import org.apache.commons.math.stat.descriptive.SummaryStatistics

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 *         Created: 28.08.2013 14:29
 */
object SummaryStatisticsBuilder {
  def fromIterable(xs: Iterable[Double]) = {
    val ss = new SummaryStatistics
    xs foreach ss.addValue
    ss
  }
}

