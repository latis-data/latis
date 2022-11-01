package latis.reader.tsml

import latis.time._
import latis.reader.tsml.ml.Tsml
import latis.data.Data
import latis.data.value.DoubleValue
import latis.data.value.StringValue

/**
 * Generate a time series of URLs based on the tsml definitions.
 * The generated URLs will have the identifier "url".
 * candidate to replace FileListGenerator2.
 */
class UrlListGenerator(tsml: Tsml) extends IterativeAdapter2[Double](tsml) {
  //TODO: reconcile with FileListGenerator (integer years)
  //TODO: allow timeRange property in tsml
  
  /**
   * Get the TimeScale for the time variable.
   */
  lazy val units: TimeScale = getOrigScalars.find(_.hasName("time")) match {
    case Some(time: Time) => time.getUnits
    case _ => throw new RuntimeException("No time variable defined.")
  }
  
  /**
   * Time step in the units of the time variable.
   */
  lazy val cadence: Double = getOrigScalars.find(_.hasName("time")) match {
    case Some(time: Time) => time.getMetadata("cadence") match {
      case Some(c) => c.toDouble //TODO: parse error
      case None => 1.0
    }
    case _ => throw new RuntimeException("No time variable defined.")
  }
  
  /**
   * Format string for URL.
   */
  lazy val format: String = tsml.getVariableAttribute("url", "pattern") //TODO: error
  
  /**
   * Create Iterator of times in numeric units from 0 to now
   * and incrementing by the optional cadence (default = 1).
   */
  def getRecordIterator: Iterator[Double] = {
    //now in given time units
    val now = Time(System.currentTimeMillis).convert(units).getNumberData.doubleValue
    // Compute number of samples.
    // Note that this violates our exclusive end bound policy.
    // Better handling of bin semantics would address this, but
    // most cases use the start of the bin which we want to include.
    val n: Int = Math.floor(now / cadence).toInt + 1 //TODO: set length in Function metadata
    Iterator.tabulate(n)(i => cadence * i)
  }
  
  /**
   * Create 'time' and 'url' data for each record (i.e. time value).
   */
  def parseRecord(rec: Double): Option[Map[String, Data]] = {
    val time = Time(units, rec).toDate
    val url = format.format(time)

    val map = Map("time" -> DoubleValue(rec), "url" -> StringValue(url))
    Some(map)
  }

  def close: Unit = {}
}
