package latis.writer

import latis.dm._

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