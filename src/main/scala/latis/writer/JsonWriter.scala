package latis.writer

import latis.dm._
import java.io._
import scala.collection.mutable.MapBuilder

class JsonWriter(out: OutputStream) extends Writer {

  /*
   * TODO: consider using arrays for tuples and samples instead of objects
   * less verbose, no labels, directly usable by Highcharts
   * effectively a multi-dim array
   * not a bad abstraction to map to since that's what so many deal with
   * Make Long and Short impls?
   *   brief, compact
   * 
   * Include metadata in this form with objects...
   */
  
  private val _writer = new PrintWriter(out)
  
  //TODO: can we generalize to writeHeader, ...?
  def write(dataset: Dataset) = {
    _writer.print("{")
    var startThenDelim = "{"
    for (v <- dataset.variables) {
      v match {
        case f: Function => writeTopLevelFunction(f)
        case _ => _writer.println(startThenDelim + varToString(v))
      }
      startThenDelim = ","
    }
    _writer.println("}")
    _writer.flush()
  }
  
  private def writeTopLevelFunction(f: Function) {
    var startThenDelim = "\"" + f.name + "\": ["
    for (Sample(domain, range) <- f.iterator) {
      val d = varToString(domain)
      val r = varToString(range)
      _writer.println(startThenDelim + "{" + d + "," + r + "}")
      startThenDelim = ","
    }
    _writer.print("]")
  }

  
  private def varToString(variable: Variable): String = variable match {
    case Scalar(v) => "\"" + variable.name + "\":" + v.toString
    case Tuple(vars) => vars.map(varToString(_)).mkString(",")
    case f: Function => ???
  }
  
  
  def close() {}
}