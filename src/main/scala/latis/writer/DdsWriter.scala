package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class DdsWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset) = "dataset {\n"
  
  override def writeVariable(variable: Variable): Unit = {
    printWriter.print(varToString(variable))
  }
  
  def makeFunction(function: Function): String = {
    count += 1
    val s = indent(count-1) + "sequence {\n" + varToString(Sample(function.getDomain, 
        function.getRange)) + indent(count-1) + "} " + function.getName + ";\n"
    count -=1
    s
  }
  
  def makeScalar(scalar:Scalar): String = scalar match {
    case Real(d) => indent(count) + "float64 " + scalar.getName + ";\n"
    case Integer(l) => indent(count) + "int64 " + scalar.getName + ";\n"
    case Text(s) => indent(count) + "string " + scalar.getName + ";\n"
    case Binary(b) => "NaN"
  }
  
  def makeTuple(tuple: Tuple): String = { 
    tuple.getVariables.map(varToString(_)).mkString("")
    /*case Sample(vars) => tuple.getVariables.map(varToString(_)).mkString("")
    case _ => {
      count += 1
      val s = indent(count-1) + "structure {\n" + tuple.getVariables.map(varToString(_)).mkString("") + indent(count-1) + "} " + tuple.getName + ";\n"
      count -=1
      s
    }*/
  }
  
  override def makeFooter(dataset: Dataset) = "} " + dataset.getName + ";\n"
  
  var count = 0
    
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 0 to num) sb append "\t"
    sb.toString
  }
    
}