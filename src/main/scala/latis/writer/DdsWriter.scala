package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class DdsWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset) = "dataset {" + newLine
  
  override def writeFunction(function: Function) {
    printWriter.print(varToString(function.getFirstSample))
  }
  
  def makeFunction(function: Function): String = varToString(function.getFirstSample)
  
  def makeScalar(scalar:Scalar): String = scalar match {
    case Real(d) => indent(tabs) + "float64 " + scalar.getName + ";\n"
    case Integer(l) => indent(tabs) + "int64 " + scalar.getName + ";\n"
    case Text(s) => indent(tabs) + "string " + scalar.getName + ";\n"
    case Binary(b) => "NaN"
  }
  
  def makeTuple(tuple: Tuple): String = tuple match {
    case Sample(d: Index, r) => varToString(r)
    case Tuple(vars) => {
      vars.map(varToString(_)).mkString(indent(tabs) + "sequence {\n\t" ,"\t", indent(tabs) + "} " + tuple.getName + ";\n")
    }
  }
  
  override def makeFooter(dataset: Dataset) = "} " + dataset.getName + ";\n"
  
  var tabs = 0
    
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 0 to num) sb append "\t"
    sb.toString
  }
}