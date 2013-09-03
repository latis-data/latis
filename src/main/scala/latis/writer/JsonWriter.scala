package latis.writer

import latis.dm._
import java.io._
import scala.collection.mutable.MapBuilder

class JsonWriter(out: OutputStream) extends Writer {

  /*
   * TODO: Include metadata in this long form with objects...
   * 
   * preserve tuples (e.g. function range), maybe overkill?
   * but what about names for unnamed structures?
   *   use "unknown"?
   *   "tuple_#"? uuid? ick
   * 
   */
  
  private val _writer = new PrintWriter(out)
  
  //TODO: can we generalize to writeHeader, ...?
  def write(dataset: Dataset) = {
    _writer.print("{\"" + dataset.name + "\":{")
    var startThenDelim = "{"
    for (v <- dataset.variables) {
      v match {
        case f: Function => writeTopLevelFunction(f)
        case _ => _writer.println(startThenDelim + varToString(v))
      }
      startThenDelim = ","
    }
    _writer.println("}}")
    _writer.flush()
  }
  
  private def writeTopLevelFunction(f: Function) {
    var startThenDelim = "\"" + f.name + "\":\n["
    for (Sample(domain, range) <- f.iterator) {
      val d = varToString(domain)
      val r = varToString(range)
      //TODO: consider dropping range tuple layer, unless it is named (other than "unknown")
      
      _writer.println(startThenDelim + "{" + d + "," + r + "}")
      startThenDelim = ","
    }
    _writer.print("]")
  }

  
  private def varToString(variable: Variable): String = {
    //TODO: impl for Function
    //TODO: what if Tuple has name? need to wrap it as an object in "{}"
    
    //assume everything has a name, may be "unknown"
    val label = "\"" + variable.name + "\":"
    
    val value = variable match { 
      //TODO: Time as unix time
      case Real(d) => d.toString //TODO: format?
      case Integer(l) => l.toString 
      case Text(s) => "\"" + s + "\"" //put quotes around text data
      case Tuple(vars) => vars.map(varToString(_)).mkString("{", ",", "}")
      case f: Function => ???
    }
    
    label + value
  }
  
  
  def close() {}
}