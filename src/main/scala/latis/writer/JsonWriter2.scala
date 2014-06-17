package latis.writer

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.util.FirstThenOther

/**
 * Write a Dataset as JSON. This is designed to be verbose with all the Metadata.
 * If you just need data values, consider the CompactJsonWriter.
 */
class JsonWriter2 extends TextWriter {
  //TODO: Include metadata in this long form with objects
  //TODO: assumes only one top level var, need to add delim
  
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
  

  /**
   * Make a label for the given Variable.
   */
  def makeLabel(variable: Variable): String = "\"" + variable.getName + "\": "

  
  /**
   * Override to add label before each variable.
   */
  override def varToString(variable: Variable): String = {
    makeLabel(variable) + super.varToString(variable) //will in turn call our make* methods below
  }
  
  /**
   * Override to escape any special characters in Text values.
   */
  override def makeScalar(scalar: Scalar): String = scalar match {
    case Real(d)    => if (d.isNaN) "null" else d.toString //replace NaNs with null //TODO: format?
    case Integer(l) => l.toString 
    case Text(s)    => "\"" + escape(s.trim) + "\"" //put quotes around text data, escape strings and control characters      
  }
  
  /**
   * Represent a Sample like a Tuple except if the domain is an Index.
   */
  override def makeSample(sample: Sample): String = sample match {
    case Sample(_: Index, r) => r match {
      case tup: Tuple => makeTuple(tup) //no label, for now //TODO: include label if tuple has name
      case _ => varToString(r)
    }
    case Sample(d, r) => "{" + varToString(d) + ", " + varToString(r) + "}" //no label for sample
  }
//  override def makeSample(sample: Sample): String = sample match {
//    case Sample(_: Index, r) => r match {
//      case tup: Tuple => makeTuple(tup) //no label, for now //TODO: include label if tuple has name
//      case _ => varToString(r)
//  override def makeSample(sample: Sample): String = {
//    val Sample(d, r) = sample
//    val rvar = r match {
//      case f: Function => varToString(f)
//      case t: Tuple => t.getVariables.map(varToString(_)).mkString(", ")
//      case _ => varToString(r)
//    }
//    val dvar = d match {
//      case _: Index => "" //drop Index domain
//      case _ => varToString(d)
//      //TODO: breaks for nested Function
//      //TODO:  Need to keep domain and range of Sample within {}
//      //  label with name or "domain"/"range"
//      //  [{"domain":{...}, "range":{...}},...]
//      //  what about index domain, need "range"?
//      //  want to avoid special logic based on whether is has a name
//      //  but often range is tuple only because range has to be a single Variable
//      //  not bad to drop extra {} if no name, still need sample {}, the only ones that can't have a label
//    }
//
////    case Sample(d, r) => "{" + varToString(d) + ", " + varToString(r) + "}" //no label for sample
//    val vars = dvar + ", " + rvar
//    vars.mkString("{","","}") //note, sample shouldn't have name
//  }
    
  /**
   * Represent a Tuple as a JSON object.
   */
  override def makeTuple(tuple: Tuple): String = {
    tuple.getVariables.map(varToString(_)).mkString("{", ", ", "}")
  }
  
  /**
   * Represent Function Samples as a JSON array.
   */
  override def makeFunction(function: Function): String = {
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
