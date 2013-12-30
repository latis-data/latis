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
  
  /*
   * catalog broken
   * has one sample: tuple, but needs {}
   * ++drop {} only it single var is function?
   * 
   * arrays of datasets and catalogs need {} around elements
   * modeled as Index functions?
   * make sure function of tuples gets {} even if unnamed
   * can elements of function be named?
   * tuple has no way of knowing that it is part of a function
   * use delim: },{ ?
   * ++need delim for dataset top vars, same as recordDelim?
   * 
   */
  
  //If Dataset has only one Variable, don't include the extra brackets.
  override def makeHeader(dataset: Dataset) = dataset.getVariables.length match {
    //case 1 => "{\"" + dataset.getName + "\": \n"  //
    /*
     * TODO: only if the var is unnamed tuple or function?
     * dataset IS-A tuple so it should have {}
     * problem seems to be lack of name
     * would this generalize better if we have a label for everything?
     * "unknown" vs gen uniq id?
     */
    case _ => "{\"" + dataset.getName + "\": {\n"
    //TODO: 0?
  }
  override def makeFooter(dataset: Dataset) = dataset.getVariables.length match {
    //case 1 => "}"
    case _ => "}}"
  }
  
//  def writeSamples(samples: Iterator[Sample], prefix: String, delim: String, suffix: String) {
//    val startThenDelim = FirstThenOther(prefix, delim)
//    for (sample <- samples) printWriter.print(startThenDelim.value + varToString(sample))
//    printWriter.print(suffix)
//  }
    
  
  override def writeFunction(function: Function) {
    val startThenDelim = FirstThenOther(makeLabel(function) + "[", "," + newLine)
    for (sample <- function.iterator) printWriter.print(startThenDelim.value + varToString(sample))
    printWriter.print("]" + newLine)
  }
  

  def makeLabel(variable: Variable): String = variable.getName match {
    //Use name for label, no label if "unknown"
    //TODO: don't count on "unknown", use Option?
    //TODO: json requires labels in some contexts
    case "unknown" => ""
    case name: String => "\"" + name + "\": "
  }
  
  override def varToString(variable: Variable): String = {
    makeLabel(variable) + super.varToString(variable) //will in turn call our make* methods below
  }
  
  
  def makeScalar(scalar: Scalar): String = scalar match {
    case t: Time => t.getJavaTime.toString  //use java time for json
    case Real(d) => d.toString //TODO: format? NaN to null
    case Integer(l) => l.toString 
    case Text(s) => "\"" + escape(s.trim) + "\"" //put quotes around text data, escape strings and control characters      
  }
  
  def makeTuple(tuple: Tuple): String = tuple match {
    case Sample(d, r) => d match {
      case _: Index => varToString(r) //drop Index domain
      case _ => {
        //Sample needs {} since we are dropping them for unnamed tuples.
        (d.getVariables ++ r.getVariables).map(varToString(_)).mkString("{", ",", "}")
      }
    }
    //Don't include brackets for unnamed tuple.
    //Note, even tuple of one will keep {} if it is named to maintain namespace.
    //TODO: Be consistent with how we make Label (e.g. drop 'unknown')
    case Tuple(vars) => tuple.getMetadata.get("name") match {
      case Some(_) => vars.map(varToString(_)).mkString("{", ",", "}")
      case None => vars.map(varToString(_)).mkString(",")
    }
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
