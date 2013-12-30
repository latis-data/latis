package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class JsonWriter extends TextWriter {
  //TODO: cleanse text, escape new lines "\\n"...
  //TODO: assumes only one top level var, need to add delim
  
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
  
  /*
   * 2013-12-30
   * catalog broken
   * has one sample: tuple, but needs {}
   * ++drop {} only if single var is function?
   * 
   * arrays of datasets and catalogs need {} around elements
   * modeled as Index functions?
   * make sure function of tuples gets {} even if unnamed
   * can elements of function be named?
   * tuple has no way of knowing that it is part of a function
   * use delim: },{ ?
   * ++need delim for dataset top vars, same as recordDelim?
   * sample should never have a name
   * careful when dropping index, wrap in anonymous tuple?
   * 
   * 
   */
  
  override def makeHeader(dataset: Dataset) = "{\"" + dataset.getName + "\": {\n"
  override def makeFooter(dataset: Dataset) = "}}"

  override def writeFunction(function: Function) {
    val startThenDelim = FirstThenOther(makeLabel(function) + "[", "," + newLine)
    //note, calling makeSample directly to avoid the label
    for (sample <- function.iterator) printWriter.print(startThenDelim.value + makeSample(sample))
    //TODO: deal with error during write, at least close "]"?
    printWriter.print("]" + newLine)
  }
  

  def makeLabel(variable: Variable): String = variable.getName match {
    //Use name for label, no label if "unknown"
    //TODO: don't count on "unknown", use Option?
    //TODO: json requires labels in some contexts
    //assume all components have names, for now, use unknown
   // case "unknown" => ""
    case name: String => "\"" + name + "\": "
  }
  
  /**
   * Override to add label before each variable.
   */
  override def varToString(variable: Variable): String = {
    makeLabel(variable) + super.varToString(variable) //will in turn call our make* methods below
  }
  
  
  def makeScalar(scalar: Scalar): String = scalar match {
    case t: Time => t.getJavaTime.toString  //use java time for json
    case Real(d) => d.toString //TODO: format? NaN to null
    case Integer(l) => l.toString 
    case Text(s) => "\"" + escape(s.trim) + "\"" //put quotes around text data, escape strings and control characters      
  }
  
  override def makeSample(sample: Sample): String = {
    val Sample(d, r) = sample
    val vars = d match {
      case _: Index => r.getVariables //drop Index domain
      case _ => d.getVariables ++ r.getVariables
      //TODO: what if d or r are named tuples?
    }
    vars.map(varToString(_)).mkString("{", ", ", "}") //note, sample shouldn't have name
  }
    
  def makeTuple(tuple: Tuple): String = {
    tuple.getVariables.map(varToString(_)).mkString("{", ", ", "}")
//    //TODO: Be consistent with how we make Label (e.g. 'unknown')
//    case Tuple(vars) => tuple.getMetadata.get("name") match {
//      case Some(_) => vars.map(varToString(_)).mkString("{", ",", "}")
//      case None => vars.map(varToString(_)).mkString(",")
//    }
  }
  
  def makeFunction(function: Function): String = {
    //note, calling makeSample directly to avoid the label
    function.iterator.map(makeSample(_)).mkString("[", ","+newLine, "]")
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
