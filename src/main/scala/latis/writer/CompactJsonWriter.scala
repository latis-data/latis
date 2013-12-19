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
    
  override def makeTuple(tuple: Tuple): String = tuple match {
    case Sample(d, r) => d match {
      case _: Index => varToString(r) //drop Index domain
      case _ => varToString(Tuple(d.getVariables ++ r.getVariables)) //combine doman and range vars into one Tuple //TODO: flatten?
    }    
    case Tuple(vars) => vars.map(varToString(_)).mkString("[", ",", "]")
  }
  
  override def mimeType: String = "application/json" 
}