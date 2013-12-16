package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class JsonWriter extends TextWriter {
  //TODO: cleanse text, escape new lines "\\n"...

  /*
   * TODO: Include metadata in this long form with objects...
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
  //def delimiter: String = 
  override def makeHeader(dataset: Dataset) = "{\"" + dataset.getName + "\":{"
  override def makeFooter(dataset: Dataset) = "}}"
  
//  def writeSamples(samples: Iterator[Sample], prefix: String, delim: String, suffix: String) {
//    val startThenDelim = FirstThenOther(prefix, delim)
//    for (sample <- samples) printWriter.print(startThenDelim.value + varToString(sample))
//    printWriter.print(suffix)
//  }
    
  
  override def writeFunctionBySample(function: Function) {
    val startThenDelim = FirstThenOther(makeLabel(function) + "[", "," + newLine)
    for (sample <- function.iterator) printWriter.print(startThenDelim.value + varToString(sample))
    printWriter.print("]" + newLine)
  }
  
//  private lazy val _writer = new PrintWriter(outputStream)
//  
//  //TODO: can we generalize to writeHeader, ...?
//  def write(dataset: Dataset) = {
//    _writer.print("{\"" + dataset.getName + "\":{")
//    var startThenDelim = "{"
//    for (v <- dataset.getVariables) {
//      v match {
//        case f: Function => writeTopLevelFunction(f)
//        case _ => _writer.println(startThenDelim + varToString(v))
//      }
//      startThenDelim = ","
//    }
//    _writer.println("}}")
//    _writer.flush()
//  }
//  
//  /**
//   * Manage top level Function so that we can write one sample at a time.
//   */
//  private def writeTopLevelFunction(f: Function) {
//    //TODO: only use label if name is defined
//    //  but need label if within {}
//    //  drop {}? 
//    var startThenDelim = "\"" + f.getName + "\":\n["
//    for (Sample(domain, range) <- f.iterator) {
//      val vars = domain.getVariables ++ range.getVariables
//      _writer.println(startThenDelim + vars.map(varToString(_)).mkString("{", ",", "}"))
//      startThenDelim = ","
//    }
//    _writer.print("]")
//  }

  def makeLabel(variable: Variable): String = variable.getName match {
    //Use name for label, no label if "unknown"
    //TODO: don't count on "unknown", use Option?
    //TODO: json requires labels in some contexts
    case "unknown" => ""
    case name: String => "\"" + name + "\":"
  }
  
  override def varToString(variable: Variable): String = {
    makeLabel(variable) + super.varToString(variable) //will in turn call our make* methods below
//    
//    val value = variable match { 
//      case t: Time => t.getJavaTime.toString  //use java time for json
//      case Real(d) => d.toString //TODO: format? NaN to null
//      case Integer(l) => l.toString 
//      case Text(s) => "\"" + escape(s.trim) + "\"" //put quotes around text data, escape strings and control characters
//      
//      case Sample(d: Index, r) => varToString(r) //don't print Index
//      case Tuple(vars) => vars.map(varToString(_)).mkString("{", ",", "}")
//      case f: Function => f.iterator.map(varToString(_)).mkString("[", ","+newLine, "]")
//      //f.iterator.map(s => (s.domain.getVariables ++ s.range.getVariables).map(varToString(_)).mkString("{",",","}")).mkString("[",",\n","]")
//    
//    }
//    
//    label + value
  }
  
  
  def makeScalar(scalar: Scalar): String = scalar match {
    case t: Time => t.getJavaTime.toString  //use java time for json
    case Real(d) => d.toString //TODO: format? NaN to null
    case Integer(l) => l.toString 
    case Text(s) => "\"" + escape(s.trim) + "\"" //put quotes around text data, escape strings and control characters      
  }
  
  def makeTuple(tuple: Tuple): String = tuple match {
    case Sample(d: Index, r) => varToString(r) //drop Index domain
    case Tuple(vars) => vars.map(varToString(_)).mkString("{", ",", "}")
  }
  
  def makeFunction(function: Function): String = {
    function.iterator.map(varToString(_)).mkString("[", ","+newLine, "]")
  }
  
  /**
   * Escape quotes and back-slashes
   */
  def escape(s: String): String = {
    //TODO: find cleaner solution
    s.replaceAllLiterally("""\""", """\\""").replaceAllLiterally(""""""", """\"""")
  }
  
  override def mimeType: String = "application/json" 
  
}