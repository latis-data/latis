package latis.writer

import latis.dm.Binary
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.Index

/**
 * Write a Dataset's DAP2 Dataset Descriptor Structure.
 * This shows the structure of the dataset but contains no data and no metadata.
 */
class DdsWriter extends TextWriter {
  
  val indentSize = 4
  
  override def makeHeader(dataset: Dataset) = "Dataset {\n"
  
  override def writeVariable(variable: Variable): Unit = {
    printWriter.print(varToString(variable))
  }
  
  /**
   * Functions are identified as Sequences of the DAP.
   */
  override def makeFunction(function: Function): String = {
    count += indentSize
    val s = indent(count-indentSize) + "Sequence {\n" + varToString(Sample(function.getDomain, 
        function.getRange)) + indent(count-indentSize) + "} " + function.getName + ";\n"
    count -= indentSize
    s
  }
  
  /**
   * Each scalar variable type maps to a DAP type:
   * 	Real    -> Float64
   *  	Integer -> Int64
   *    Index   -> Int 64
   *    Text    -> String
   */
  override def makeScalar(scalar:Scalar): String = scalar match {
    case _: Real    => indent(count) + "Float64 " + scalar.getName + ";\n"
    case _: Integer => indent(count) + "Int32 "   + scalar.getName + ";\n"
    case _: Text    => indent(count) + "String "  + scalar.getName + ";\n"
    case _: Binary  => "NaN"
    case _: Index => indent(count) + "Int32 " + scalar.getName + ";\n"
  }
  
  /**
   * No structure for a Sample. Just considers inner Variables.
   */
  override def makeSample(sample: Sample): String = {
    sample.getVariables.map(varToString(_)).mkString("")
  }
  
  /**
   * Tuples correspond to the DAP Structure type.
   */
  override def makeTuple(tuple: Tuple): String = { 
    count += indentSize
    val s = indent(count-indentSize) + "Structure {\n" + tuple.getVariables.map(varToString(_)).mkString("") + indent(count-indentSize) + "} " + tuple.getName + ";\n"
    count -= indentSize
    s
  }
  
  override def makeFooter(dataset: Dataset) = "} " + dataset.getName + ";"
  
  var count = indentSize
  
  /**
   * Use a counter to indent each line by the appropriate amount.
   */
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 1 to num) sb append " "
    sb.toString
  }
    
}
