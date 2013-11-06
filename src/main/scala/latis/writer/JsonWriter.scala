package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder

class JsonWriter extends Writer {
  //TODO: extend TextWriter?
  //TODO: cleanse text, escape new lines "\\n"...

  /*
   * TODO:import latis.time.Time
 Include metadata in this long form with objects...
   * 
   * preserve tuples (e.g. function range), maybe overkill?
   * but what about names for unnamed structures?
   *   use "unknown"?
   *   "tuple_#"? uuid? ick
   * This should be a non-ambiguous representation of the LaTiS data model
   *   A Function is a sequence (json array) of domain, range pairs
   *   so range needs to be a tuple (if multiple vars)
   *   otherwise need convention saying that first var is domain and the rest the range
   */
  
  private lazy val _writer = new PrintWriter(outputStream)
  
  //TODO: can we generalize to writeHeader, ...?
  def write(dataset: Dataset, args: Seq[String]) = {
    _writer.print("{\"" + dataset.getName + "\":{")
    var startThenDelim = "{"
    for (v <- dataset.getVariables) {
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
    var startThenDelim = "\"" + f.getName + "\":\n["
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
    
    //assume everything has a name, may be "unknown"
    val label = "\"" + variable.getName + "\":"
    
    val value = variable match { 
      case Index(i) => i.toString //TODO: exclude implicit Index
      case t: Time => t.getJavaTime.toString  //use java time for json
      case Real(d) => d.toString //TODO: format?
      case Integer(l) => l.toString 
      case Text(s) => "\"" + s.trim + "\"" //put quotes around text data
      case Tuple(vars) => vars.map(varToString(_)).mkString("{", ",", "}")
      case f: Function => ???
    }
    
    label + value
  }
  
  
  override def mimeType: String = "application/json" 
  
  def close() {}
}