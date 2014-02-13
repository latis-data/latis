package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class DasWriter extends TextWriter {
  
  val indentSize = 4
  
  override def makeHeader(dataset: Dataset) = "attributes {" + newLine
  
  override def writeVariable(variable: Variable): Unit = {
    printWriter.print(varToString(variable))
  }
  
  def makeAttributes(variable: Variable): String = {
    val props = variable.getMetadata.getProperties
    count-=indentSize
    if (props.filterNot(_._1 == "name").nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name"))
        yield indent(count+indentSize) + "string " + name + " \"" + value + "\""   //metadata should only be strings
      ss.mkString("", ";\n", ";\n" + indent(count) + "}\n")
    }
    else indent(count) + "}\n"
  }

  override def makeFunction(function: Function): String = varToString(Sample(function.getDomain, function.getRange))
  
  override def makeScalar(scalar:Scalar): String = {
    makeLabel(scalar) + makeAttributes(scalar)
  }
  
  override def makeTuple(tuple: Tuple): String = {
    val label = makeLabel(tuple)
    val s = tuple.getVariables.map(varToString(_))
    count-=indentSize
    label + s.mkString("","",indent(count)+"}\n")
  }
  
  override def makeFooter(dataset: Dataset) = "}"
  
  var count = 0
    
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 0 to num) sb append " "
    sb.toString
  }
  
  def makeLabel(variable: Variable): String ={
    count +=indentSize
    indent(count-indentSize) + variable.getName + "{\n"
  }
}