package latis.writer

import latis.dm._

class ProtobufWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset) = "message " + toCamelCase(dataset.getName) + " {" + newLine
    
  override def makeFooter(dataset: Dataset) = "}"
  
  override def writeVariable(variable: Variable) = printWriter.print(varToString(variable))
    
  override def varToString(variable: Variable) = {
    makeMessage(variable) + makeOpLabel(variable)
  }
  
  def makeScalarLabel(scalar: Scalar) = {
    val name = scalar.getName
    scalar match{
      case _: Index   => ""
      case _: Real    => indent(count) + "optional float " + name + " = " + tag + ";" + newLine
      case _: Integer => indent(count) + "optional int32 " + name + " = " + tag + ";" + newLine
      case _: Text    => indent(count) + "optional string " + name + " = " + tag + ";" + newLine
      case _: Binary  => ""
    }
  }
  
  def makeScalarRep(scalar: Scalar) = {
    val name = scalar.getName
    scalar match{
      case _: Index   => ""
      case _: Real    => indent(count) + "repeated float " + name + " = " + tag + ";" + newLine
      case _: Integer => indent(count) + "repeated int32 " + name + " = " + tag + ";" + newLine
      case _: Text    => indent(count) + "repeated string " + name + " = " + tag + ";" + newLine
      case _: Binary  => ""
    }
  }
  
  def makeTupleMessage(tuple: Tuple) = {
    val temp = tag
    tag = 0
    count += indentSize
    val s = tuple match {
      case Sample(d: Index, r) => varToString(r)
      case Tuple(vars) => indent(count-indentSize) + "message " + toCamelCase(tuple.getName) + " {" + newLine + 
                          vars.map(varToString(_)).mkString("") + 
                          indent(count-indentSize) + "}" + newLine
    }
    tag = temp 
    count -= indentSize
    s
  }
  
  def makeFunctionMessage(function: Function) = {
    val temp = tag
    tag = 0
    count += indentSize
    val x = makeMessage(function.getDomain) + makeRepLabel(function.getDomain)
    val y = makeMessage(function.getRange) + makeRepLabel(function.getRange)
    count -= indentSize
    tag = temp
    
    indent(count) + "message " + toCamelCase(function.getName) + " {" + newLine + x + y + indent(count) + "}" + newLine
        
  }
  
  def makeOpLabel(variable: Variable) = {
    tag += 1
    variable match {
      case s: Scalar => makeScalarLabel(s)
      case t: Tuple => indent(count) + "optional " + toCamelCase(t.getName) + " " + t.getName + " = " + tag + ";" + newLine
      case f: Function => indent(count) + "optional " + toCamelCase(f.getName) + " " + f.getName + " = " + tag + ";" + newLine
    }
  }
  
  def makeRepLabel(variable: Variable) = {
    tag += 1
    variable match {
      case s: Scalar => makeScalarRep(s)
      case t: Tuple => indent(count) + "repeated " + toCamelCase(t.getName) + " " + t.getName + " = " + tag + ";" + newLine
      case f: Function => indent(count) + "repeated " + toCamelCase(f.getName) + " " + f.getName + " = " + tag + ";" + newLine  
    }
  }
  
  def makeMessage(variable: Variable): String = variable match{
    case   scalar: Scalar   => ""
    //case   sample: Sample   => makeSample(sample)
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
    
}