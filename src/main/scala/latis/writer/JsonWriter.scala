package latis.writer

import latis.dm._
import java.io._
import scala.collection.mutable.MapBuilder

class JsonWriter(out: OutputStream) extends Writer {

  /*
   * TODO: Include metadata in this long form with objects...
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

  
  private def varToString(variable: Variable): String = {
    //TODO: impl for Function
    //TODO: what if Tuple has name? need to wrap it as an object in "{}"
    
    val label = variable.name match {
      case name: String => "\"" + name + "\":"
      case _ => ""
    } 
    
    val value = variable match { 
      case Number(d) => d.toString //TODO: format?
      //TODO: Integer vs Real?
      case Text(s) => "\"" + s + "\"" //put quotes around text data
      case Tuple(vars) => vars.map(varToString(_)).mkString(",")
      case f: Function => ???
    }
    
    label + value
  }
  
  
  def close() {}
}