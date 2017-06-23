package latis.reader.tsml

import latis.dm._
import latis.time._
import latis.reader.tsml.ml.Tsml
import latis.util.StringUtils
import latis.data.Data
import latis.data.value.DoubleValue
import latis.data.value.StringValue

/**
 * Generate a time series of URLs based on the tsml definitions.
 * See latis-noms gfsanl_wind_urls.tsml
 */
class FileListGenerator2(tsml: Tsml) extends IterativeAdapter2[Double](tsml) {
  //NOTE: we need to use "file" (as opposed to "url") for the ZipWriter2 to work with this
  //TODO: reconcile with FileListGenerator (integer years)
  //TODO: allow timeRange property in tsml
  
  /**
   * Get the TimeScale for the time variable.
   */
  lazy val units: TimeScale = getOrigScalars.find(_.hasName("time")) match {
    case Some(time: Time) => time.getUnits
    case _ => throw new Error("No time variable defined.")
  }
  
  /**
   * Time step in the units of the time variable.
   */
  lazy val cadence: Double = getOrigScalars.find(_.hasName("time")) match {
    case Some(time: Time) => time.getMetadata("cadence") match {
      case Some(c) => c.toDouble //TODO: parse error
      case None => 1.0
    }
    case _ => throw new Error("No time variable defined.")
  }
  
  /**
   * Format string for URL.
   */
  lazy val format: String = tsml.getVariableAttribute("file", "pattern") //TODO: error
  
  /**
   * Create Iterator of times in numeric units from 0 to now
   * and incrementing by the optional cadence (default = 1).
   */
  def getRecordIterator: Iterator[Double] = {
    //now in given time units
    val now = Time(System.currentTimeMillis).convert(units).getNumberData.doubleValue
    val n: Int = Math.floor(now / cadence).toInt  //number of samples //TODO: set length in Function metadata
    //Iterator.iterate(0.0)(i => i * cadence) //infinite!
    Iterator.tabulate(n)(i => cadence * i)
  }
  
  /**
   * Create 'time' and 'file' data for each record (i.e. time value).
   */
  def parseRecord(rec: Double): Option[Map[String, Data]] = {
    val time = Time(units, rec).toDate
    val file = format.format(time)

    val map = Map("time" -> DoubleValue(rec), "file" -> StringValue(file))
    Some(map)
  }

  def close: Unit = {}
}