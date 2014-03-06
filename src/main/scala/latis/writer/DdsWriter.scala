package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class DdsWriter extends TextWriter {
  
  val indentSize = 4
  
  override def makeHeader(dataset: Dataset) = "Dataset {\n"
  
  override def writeVariable(variable: Variable): Unit = {
    printWriter.print(varToString(variable))
  }
  
  override def makeFunction(function: Function): String = {
    count += indentSize
    val s = indent(count-indentSize) + "Sequence {\n" + varToString(Sample(function.getDomain, 
        function.getRange)) + indent(count-indentSize) + "} " + function.getName + ";\n"
    count -= indentSize
    s
  }
  
  override def makeScalar(scalar:Scalar): String = scalar match {
    case _: Real    => indent(count) + "Float64 " + scalar.getName + ";\n"
    case _: Integer => indent(count) + "Int64 "   + scalar.getName + ";\n"
    case _: Text    => indent(count) + "String "  + scalar.getName + ";\n"
    case _: Binary  => "NaN"
  }
  
  override def makeTuple(tuple: Tuple): String = tuple match{ 
    case Sample(vars) => tuple.getVariables.map(varToString(_)).mkString("")
    case _ => {
      count += indentSize
      val s = indent(count-indentSize) + "Structure {\n" + tuple.getVariables.map(varToString(_)).mkString("") + indent(count-indentSize) + "} " + tuple.getName + ";\n"
      count -= indentSize
      s
    }
  }
  
  override def makeFooter(dataset: Dataset) = "} " + dataset.getName + ";\n"
  
  var count = indentSize
  
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 1 to num) sb append " "
    sb.toString
  }
    
}