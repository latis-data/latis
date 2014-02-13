package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class DdsWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset) = "dataset {\n"
  
  override def writeVariable(variable: Variable): Unit = {
    printWriter.print(varToString(variable))
  }
  
  override def makeFunction(function: Function): String = {
    count += 1
    val s = indent(count-1) + "sequence {\n" + varToString(Sample(function.getDomain, 
        function.getRange)) + indent(count-1) + "} " + function.getName + ";\n"
    count -=1
    s
  }
  
  override def makeScalar(scalar:Scalar): String = scalar match {
    case _: Real    => indent(count) + "float64 " + scalar.getName + ";\n"
    case _: Integer => indent(count) + "int64 "   + scalar.getName + ";\n"
    case _: Text    => indent(count) + "string "  + scalar.getName + ";\n"
    case _: Binary  => "NaN"
  }
  
  override def makeTuple(tuple: Tuple): String = tuple match{ 
    case Sample(vars) => tuple.getVariables.map(varToString(_)).mkString("")
    case _ => {
      count += 1
      val s = indent(count-1) + "structure {\n" + tuple.getVariables.map(varToString(_)).mkString("") + indent(count-1) + "} " + tuple.getName + ";\n"
      count -=1
      s
    }
  }
  
  override def makeFooter(dataset: Dataset) = "} " + dataset.getName + ";\n"
  
  var count = 0
    
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 0 to num) sb append "\t"
    sb.toString
  }
    
}