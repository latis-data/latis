package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

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
  
  lazy val delimiter: String = {
    //TODO: get from properties
    ","
  }
  
  def write(dataset: Dataset) {
    writeHeader(dataset)
    dataset.getVariables.map(writeVariable(_))
    writeFooter(dataset)
    printWriter.flush()
  }
  
  def writeVariable(variable: Variable): Unit = variable match {
    case f: Function => {
      var startThenDelim = ""
      for (sample <- f.iterator)  {
        //new line for each record
        printWriter.println(startThenDelim + varToString(sample))
        startThenDelim = delimiter
      }
    }
    case _ => printWriter.println(varToString(variable)) //new line for each top level variable
  }
  
  def varToString(variable: Variable): String 
  
  def writeHeader(dataset: Dataset) = printWriter.print(makeHeader(dataset)) //NOTE: just "print" w/o nl
  def writeFooter(dataset: Dataset) = printWriter.print(makeFooter(dataset)) //NOTE: just "print" w/o nl
  
  def makeHeader(dataset: Dataset): String = ""
  def makeFooter(dataset: Dataset): String = ""
}
