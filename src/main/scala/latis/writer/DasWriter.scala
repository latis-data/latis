package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class DasWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset) = "attributes {" + newLine
  
  override def writeFunction(function: Function) {
    //printWriter.print(varToString(function.getFirstSample))
   // val sb = new StringBuilder()
   // sb append("\t" + function.getName + " {\n")
    //tabs += 1
    //tabs -= 1
   // sb.append("\t}\n").toString
    printWriter.print(varToString(function.getFirstSample) + "\t}\n")
  }
  
  override def varToString(variable: Variable): String = {
    val sb = new StringBuilder()
    variable match {
      case s: Scalar => sb append makeLabel(variable) + makeAttributes(variable)
      case Tuple(vars) => sb append makeLabel(variable) + vars.map(varToString(_)).mkString("")
      case Function(d,r) => sb append makeLabel(variable) + varToString(Sample(d,r))
    }
    sb.toString()
  }
  
  def makeAttributes(variable: Variable): String = {
    val props = variable.getMetadata.getProperties
    tabs-=1
    if (props.filterNot(_._1 == "name").nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name")) //don't include name (redundant)
        yield indent(tabs+1) + "string " + name + " \"" + value + "\""   //metadata is only strings for now, could change later
      ss.mkString("", ";\n", ";\n" + indent(tabs) + "}\n")
    }
    else indent(tabs) + "}\n"
  }
  
  def makeFunction(function: Function): String = varToString(Sample(function.getDomain, function.getRange))
  
  
  def makeScalar(scalar:Scalar): String = scalar match {
    case Real(d) => scalar.getName + " {" + newLine + makeAttributes(scalar)
    case Integer(l) => "int64 " + scalar.getName + ";" + newLine
    case Text(s) => "string " + scalar.getName + ";" + newLine
    case Binary(b) => "NaN"
  }
  
  def makeTuple(tuple: Tuple): String = tuple match {
    case Sample(d: Index, r) => varToString(r)
    case Tuple(vars) => vars.map(varToString(_)).mkString("sequence {" + newLine ,"", "} " + tuple.getName + ";" + newLine)
  }
  
  override def makeFooter(dataset: Dataset) = "}\n"
  
  var tabs = 0
    
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 0 to num) sb append "\t"
    sb.toString
  }
  
  def makeLabel(variable: Variable): String ={
    tabs +=1
    indent(tabs-1) + variable.getName + "{\n"
  }
}