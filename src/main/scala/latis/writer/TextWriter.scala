package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter
import latis.util.FirstThenOther

abstract class TextWriter extends Writer {
  //TODO: with Text, Binary trait?
  //TODO: build with Dataset? but write(ds) feels better that write()
  //TODO: Dataset.write ?
  //TODO: with Header
  
  /*
   * TODO: generalize to all writers?
   * write vs makeString
   * but makeString seems more reusable
   * use mkString?
   * use record semantics?
   * top level scalars and tuples, top level function samples
   * buildRecord, varToString...?
   */
  
  private[this] lazy val _writer = new PrintWriter(outputStream)
  def printWriter: PrintWriter = _writer
  //TODO: what happens to PrintWriter when out is closed?
  
  //TODO: platform indep new line
  val newLine = "\n"
  

//  def recordDelim = newLine
//  def fieldDelim = ","
  
  //def startThenDelim = FirstThenOther("", newLine)
    
  
  def writeHeader(dataset: Dataset) = printWriter.print(makeHeader(dataset)) //NOTE: just "print" w/o nl
  def writeFooter(dataset: Dataset) = printWriter.print(makeFooter(dataset)) //NOTE: just "print" w/o nl
  
  def makeHeader(dataset: Dataset): String = ""
  def makeFooter(dataset: Dataset): String = ""
    
  /**
   * Main entry point for writing a Dataset.
   */
  def write(dataset: Dataset) {
    writeHeader(dataset)
    dataset.getVariables.map(writeVariable(_)) //TODO: add hook for delimiter (other than new line) between top level vars
    writeFooter(dataset)
    printWriter.flush()
  }
  
  /**
   * Write the given Variable.
   * Designed for top level Variables so we can support arbitrarily large datasets.
   * Otherwise, use "make*" to create a String.
   */
  def writeVariable(variable: Variable): Unit = variable match {
    case f: Function => writeFunction(f)
    case _ => printWriter.println(varToString(variable)) //new line for each top level variable
  }
  
  /**
   * Write the given Function one sample at a time.
   * Designed for top level Functions so we can support arbitrarily large datasets.
   * Otherwise, use "makeFunction" to create a String.
   */
  def writeFunction(function: Function) {
    val startThenDelim = FirstThenOther("", newLine)
    for (sample <- function.iterator) printWriter.print(startThenDelim.value + varToString(sample))
    printWriter.println
  }
  
  //make lots of extension points
  //TODO: default impl?
  //TODO: or just override with other cases delegating to super?
  
  def varToString(variable: Variable): String = variable match {
    case   scalar: Scalar   => makeScalar(scalar)
    case   sample: Sample   => makeSample(sample)
    case    tuple: Tuple    => makeTuple(tuple)
    case function: Function => makeFunction(function)
  }
  
  def makeScalar(scalar: Scalar): String 
  def makeSample(sample: Sample): String = makeTuple(sample)
  def makeTuple(tuple: Tuple): String 
  def makeFunction(function: Function): String
  
//  = scalar match {
//    case real: Real => makeReal(real)
//    case int: Integer => makeInteger(int)
//    case text: Text => makeText(text)
//    case bin: Binary => makeBinary(bin)
//  }
//    case Real(d) => d.toString
//    case Integer(l) => l.toString
//    case Text(s) => s.trim
//    case Binary(b) => "NaN" //TODO: uuencode?
//    //TODO: use Scalar.toStringValue?
//  //TODO: deal with Time format
    
//  = tuple match {
//    case Sample(d: Index, r) => varToString(r) //drop Index domain
//    case Tuple(vars) => vars.map(varToString(_)).mkString(delimiter)
//  }
  
 

  override def mimeType: String = "text/plain"
}
