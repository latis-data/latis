package latis.writer

import latis.dm._
import java.io._
import scala.collection.mutable.MapBuilder

/**
 * Return data as nested arrays, without metadata.
 * One inner array for each sample.
 * Handy for clients that just want the data (e.g. HighCharts).
 */
class CompactJsonWriter(out: OutputStream) extends Writer {
  //TODO: mime type
  
  private val _writer = new PrintWriter(out)
  
  //TODO: can we generalize to writeHeader, ...?
  def write(dataset: Dataset) = {
    _writer.print("[")
    
    //assume a single top level Function
    val f = dataset.variables.find(_.isInstanceOf[Function]) match {
      case Some(f: Function) => f
      case _ => throw new RuntimeException("No Function found in dataset: " + dataset)
    }
    
    writeTopLevelFunction(f)
    
    _writer.println("]")
    _writer.flush()
  }
  
  private def writeTopLevelFunction(f: Function) {
    var startThenDelim = ""
    for (Sample(domain, range) <- f.iterator) {
      val d = varToString(domain)
      val r = varToString(range)
      //TODO: reduce resolution to keep volume down?
      _writer.println(startThenDelim + "[" + d + "," + r + "]")
      startThenDelim = ","
    }
  }

  
  private def varToString(variable: Variable): String = variable match {
    case Scalar(v) => v.toString
    case Tuple(vars) => vars.map(varToString(_)).mkString(",")
    case f: Function => ??? //TODO: deal with inner Function
  }
  
  
  def close() {}
}