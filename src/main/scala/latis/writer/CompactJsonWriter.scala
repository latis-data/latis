package latis.writer

import latis.dm._
import latis.time.Time

/**
 * Return data as nested arrays, without metadata.
 * One inner array for each sample.
 * Handy for clients that just want the data (e.g. HighCharts).
 * Like CSV, table as 2D array.
 */
class CompactJsonWriter extends JsonWriter {

  override def makeHeader(dataset: Dataset) = ""
  override def makeFooter(dataset: Dataset) = ""
  
  override def makeLabel(variable: Variable): String = ""
    
  /**
   * Override to present time in native JavaScript units: milliseconds since 1970.
   */
  override def makeScalar(scalar: Scalar): String = scalar match {
    case t: Time => t.getJavaTime.toString  //use java time for json
    case _ => super.makeScalar(scalar)
  }
  
  override def makeSample(sample: Sample): String = {
    val Sample(d, r) = sample
    d match {
      case _: Index => varToString(r) //drop Index domain
      case _ => varToString(Tuple(d.getVariables ++ r.getVariables)) //combine domain and range vars into one Tuple //TODO: flatten?
    }
  }
    
  override def makeTuple(tuple: Tuple): String = {
    tuple.getVariables.map(varToString(_)).mkString("[", ",", "]") //represent a tuple as an array
  }
}