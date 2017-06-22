package latis.writer

import latis.dm._

/**
 * Writes the .proto file that describes the dataset but contains no data or metadata.
 */
class ProtoWriter extends TextWriter {
  
  override def makeHeader(dataset: Dataset): String = "message " + toCamelCase(dataset.getName) + " {" + newLine
    
  override def makeFooter(dataset: Dataset): String = "}"
  
  override def writeVariable(variable: Variable): Unit = printWriter.print(varToString(variable))
  
  override def varToString(variable: Variable): String = {
    makeMessage(variable) + makeOpLabel(variable)
  }
  
  /**
   * Provided for repeated variables in functions.
   */
  def varToRepString(variable: Variable): String = makeMessage(variable) + makeRepLabel(variable)
  
  /**
   * Matches each scalar type to an appropriate protobuf type:
   *   Real -> double
   *   Integer -> int64
   *   Text -> string
   *   (Indexes are dropped)
   */
  override def makeScalar(scalar: Scalar): String = {
    val name = scalar.getName
    scalar match{
      case _: Index   => ""
      case _: Real    => indent(count) + "optional double " + name + " = " + tag + ";" + newLine
      case _: Integer => indent(count) + "optional int64 " + name + " = " + tag + ";" + newLine
      case _: Text    => indent(count) + "optional string " + name + " = " + tag + ";" + newLine
      case _: Binary  => indent(count) + "optional bytes " + name + " = " + tag + ";" + newLine
    }
  }
  
  /**
   * Makes the label for a repeated scalar.
   */
  def makeScalarRep(scalar: Scalar): String = {
    val name = scalar.getName
    scalar match{
      case _: Index   => ""
      case _: Real    => indent(count) + "repeated float " + name + " = " + tag + ";" + newLine
      case _: Integer => indent(count) + "repeated int64 " + name + " = " + tag + ";" + newLine
      case _: Text    => indent(count) + "repeated string " + name + " = " + tag + ";" + newLine
      case _: Binary  => indent(count) + "repeated bytes " + name + " = " + tag + ";" + newLine
    }
  }
  
  /**
   * Ignores the sample and looks at the inner variables
   */
  override def makeSample(sample: Sample): String = {
    val temp = tag
    tag = 0
    val s = sample match {
      case Sample(d, r: Tuple) => varToRepString(d) + r.getVariables.map(varToRepString(_)).mkString("")
      case _ => sample.getVariables.map(varToRepString(_)).mkString("")
    }
    tag = temp
    s
  }
  
  /**
   * Makes a message of the tuple. 
   */
  override def makeTuple(tuple: Tuple): String = {
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
  
 /**
  * Make a message from a function. 
  */
  override def makeFunction(function: Function): String = {
    val temp = tag
    tag = 0
    count += indentSize
    val s = varToRepString(Sample(function.getDomain, function.getRange))
    count -= indentSize
    tag = temp
    
    indent(count) + "message " + toCamelCase(function.getName) + " {" + newLine + s + indent(count) + "}" + newLine
        
  }
  
  /**
   * Makes an optional label for a variable.
   */
  def makeOpLabel(variable: Variable): String = {
    tag += 1
    variable match {
      case s: Scalar => makeScalar(s)
      case _: Sample => ""
      case t: Tuple => indent(count) + "optional " + toCamelCase(t.getName) + " " + t.getName + " = " + tag + ";" + newLine
      case f: Function => indent(count) + "optional " + toCamelCase(f.getName) + " " + f.getName + " = " + tag + ";" + newLine
    }
  }
  
  /**
   * Makes a repeated label for a variable.
   */
  def makeRepLabel(variable: Variable): String = {
    tag += 1
    variable match {
      case s: Scalar => makeScalarRep(s)
      case _: Sample => ""
      case t: Tuple => indent(count) + "repeated " + toCamelCase(t.getName) + " " + t.getName + " = " + tag + ";" + newLine
      case f: Function => indent(count) + "repeated " + toCamelCase(f.getName) + " " + f.getName + " = " + tag + ";" + newLine  
    }
  }
  
  /**
   * For non-scalar Variables, creates a nested message.
   */
  def makeMessage(variable: Variable): String = variable match{
    case   scalar: Scalar   => ""
    case   sample: Sample   => makeSample(sample)
    case    tuple: Tuple    => makeTuple(tuple)
    case function: Function => makeFunction(function)
  }
  
  /**
   * Uses a counter to indent each line by the proper amount.
   */
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
