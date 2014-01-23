package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class DasWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset) = "attributes {" + newLine
  
  override def writeFunction(function: Function) {   
    printWriter.print(varToString(function))
  }
  
  /*override def varToString(variable: Variable): String = {
    
    val sb = new StringBuilder()
    variable match {
      case s: Scalar => sb append makeLabel(variable) + makeAttributes(variable)
      case Tuple(vars) => {
        if(variable.isInstanceOf[Dataset]) sb append vars.map(varToString(_)).mkString("")
        else sb append makeLabel(variable) + vars.map(varToString(_)).mkString("")
      }
      case Function(d,r) => sb append makeLabel(variable) + varToString(Sample(d,r))
    }
    sb.toString()
  }*/
  
  def makeAttributes(variable: Variable): String = {
    val props = variable.getMetadata.getProperties
    count-=1
    if (props.filterNot(_._1 == "name").nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name")) //don't include name (redundant)
        yield indent(count+1) + "string " + name + " \"" + value + "\""   //metadata is only strings for now, could change later
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
  
  override def makeFooter(dataset: Dataset) = "}\n"
  
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