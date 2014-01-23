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
    printWriter.print(makeLabel(function) + "[")
    val startThenDelim = FirstThenOther("", "," + newLine)
    //note, calling makeSample directly to avoid the label
    for (sample <- function.iterator) printWriter.print(startThenDelim.value + makeSample(sample))
    //TODO: deal with error during write, at least close "]"?
    printWriter.print("]" + newLine)
  }
  

  def makeLabel(variable: Variable): String = "\"" + variable.getName + "\": "
//    variable.getName match {
//    //TODO: don't count on "unknown", use Option?
//    //json requires labels in some contexts
//    //assume all components have names, for now, use whatever we get
//    //TODO: if unknown, at least use type? e.g. "function"? or just delegate to Var.getName?
//    case name: String => "\"" + name + "\": "
//  }
  
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
      //TODO: breaks for nested Function
      //TODO:  Need to keep domain and range of Sample within {}
      //  label with name or "domain"/"range"
      //  [{"domain":{...}, "range":{...}},...]
      //  what about index domain, need "range"?
      //  want to avoid special logic based on whether is has a name
      //  but often range is tuple only because range has to be a single Variable
      //  not bad to drop extra {} if no name, still need sample {}, the only ones that can't have a label
    }
    vars.map(varToString(_)).mkString("{", ", ", "}") //note, sample shouldn't have name
  }
    
  def makeTuple(tuple: Tuple): String = {
    tuple.getVariables.map(varToString(_)).mkString("{", ", ", "}")
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
