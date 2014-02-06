package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class DasWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset) = "attributes {" + newLine
  
  override def writeVariable(variable: Variable): Unit = {
    printWriter.print(varToString(variable))
  }
  
  def makeAttributes(variable: Variable): String = {
    val props = variable.getMetadata.getProperties
    count-=1
    if (props.filterNot(_._1 == "name").nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name"))
        yield indent(count+1) + "string " + name + " \"" + value + "\""   //metadata should only be strings
      ss.mkString("", ";\n", ";\n" + indent(count) + "}\n")
    }
    else indent(count) + "}\n"
  }
  
  def makeFunction(function: Function): String = varToString(function.getFirstSample)
  
  def makeScalar(scalar:Scalar): String = {
    makeLabel(scalar) + makeAttributes(scalar)
  }
  
  def makeTuple(tuple: Tuple): String = {
    val label = makeLabel(tuple)
    val s = tuple.getVariables.map(varToString(_))
    count-=1
    label + s.mkString("","",indent(count)+"}\n")
  }
  
  override def makeFooter(dataset: Dataset) = "}"
  
  var count = 0
    
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 0 to num) sb append "\t"
    sb.toString
  }
  
  def makeLabel(variable: Variable): String ={
    count +=1
    indent(count-1) + variable.getName + "{\n"
  }
}