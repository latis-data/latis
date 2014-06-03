package latis.writer

import latis.dm._

class ProtoWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset) = "message " + toCamelCase(dataset.getName) + " {" + newLine
    
  override def makeFooter(dataset: Dataset) = "}"
  
  override def writeVariable(variable: Variable) = printWriter.print(varToString(variable))
    
  override def varToString(variable: Variable) = {
    makeMessage(variable) + makeOpLabel(variable)
  }
  
  def varToRepString(variable: Variable) = makeMessage(variable) + makeRepLabel(variable)
  
  def makeScalarLabel(scalar: Scalar) = {
    val name = scalar.getName
    scalar match{
      case _: Index   => ""
      case _: Real    => indent(count) + "optional double " + name + " = " + tag + ";" + newLine
      case _: Integer => indent(count) + "optional int64 " + name + " = " + tag + ";" + newLine
      case _: Text    => indent(count) + "optional string " + name + " = " + tag + ";" + newLine
      case _: Binary  => ""
    }
  }
  
  def makeScalarRep(scalar: Scalar) = {
    val name = scalar.getName
    scalar match{
      case _: Index   => ""
      case _: Real    => indent(count) + "repeated float " + name + " = " + tag + ";" + newLine
      case _: Integer => indent(count) + "repeated int64 " + name + " = " + tag + ";" + newLine
      case _: Text    => indent(count) + "repeated string " + name + " = " + tag + ";" + newLine
      case _: Binary  => ""
    }
  }
  
  def makeSampleMessage(sample: Sample) = {
    val temp = tag
    tag = 0
    val s = sample match {
      case Sample(d, r: Tuple) => varToRepString(d) + r.getVariables.map(varToRepString(_)).mkString("")
      case _ => sample.getVariables.map(varToRepString(_)).mkString("")
    }
    tag = temp
    s
  }
  
  def makeTupleMessage(tuple: Tuple) = {
    val temp = tag
    tag = 0
    count += indentSize
    val vars = tuple.getVariables
    val s = indent(count-indentSize) + "message " + toCamelCase(tuple.getName) + " {" + newLine + 
            vars.map(varToString(_)).mkString("") + 
            indent(count-indentSize) + "}" + newLine
    tag = temp 
    count -= indentSize
    s
  }
  
  def makeFunctionMessage(function: Function) = {
    val temp = tag
    tag = 0
    count += indentSize
    val s = varToRepString(Sample(function.getDomain, function.getRange))
    count -= indentSize
    tag = temp
    
    indent(count) + "message " + toCamelCase(function.getName) + " {" + newLine + s + indent(count) + "}" + newLine
        
  }
  
  def makeOpLabel(variable: Variable) = {
    tag += 1
    variable match {
      case s: Scalar => makeScalarLabel(s)
      case _: Sample => ""
      case t: Tuple => indent(count) + "optional " + toCamelCase(t.getName) + " " + t.getName + " = " + tag + ";" + newLine
      case f: Function => indent(count) + "optional " + toCamelCase(f.getName) + " " + f.getName + " = " + tag + ";" + newLine
    }
  }
  
  def makeRepLabel(variable: Variable) = {
    tag += 1
    variable match {
      case s: Scalar => makeScalarRep(s)
      case _: Sample => ""
      case t: Tuple => indent(count) + "repeated " + toCamelCase(t.getName) + " " + t.getName + " = " + tag + ";" + newLine
      case f: Function => indent(count) + "repeated " + toCamelCase(f.getName) + " " + f.getName + " = " + tag + ";" + newLine  
    }
  }
  
  def makeMessage(variable: Variable): String = variable match{
    case   scalar: Scalar   => ""
    case   sample: Sample   => makeSampleMessage(sample)
    case    tuple: Tuple    => makeTupleMessage(tuple)
    case function: Function => makeFunctionMessage(function)
  }
  
  def indent(num: Int): String = {
    " " * num
  }
  
  val indentSize = 4
  var tag = 0
  var count = indentSize
  
  def toCamelCase(string: String): String = {
    string.split('_').map(_.capitalize).mkString("")
  }
    
  override def mimeType: String = "text/proto" 

}