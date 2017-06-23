package latis.writer

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable

/**
 * Write a Dataset's DAP2 Dataset Attribute Structure.
 * Contains only metadata, no data
 */
class DasWriter extends TextWriter {
  
  val indentSize = 4
  
  override def makeHeader(dataset: Dataset): String = "attributes {" + newLine

  override def writeVariable(variable: Variable): Unit = {
    printWriter.print(varToString(variable))
  }

  /**
   * Makes the metadata string for a variable. 
   */
  def makeAttributes(variable: Variable): String = {
    val props = variable.getMetadata.getProperties
    count-=indentSize
    if (props.filterNot(_._1 == "name").nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name"))
        yield indent(count+indentSize) + "string " + name + " \"" + value + "\"" 
      ss.mkString("", ";\n", ";\n" + indent(count) + "}\n")
    }
    else indent(count) + "}\n"
  }

  /**
   * Avoids unnecessary iteration of the function.
   */
  override def makeFunction(function: Function): String = {
    val label = makeLabel(function) 
    val s = varToString(Sample(function.getDomain, function.getRange))
    count-=indentSize
    label + s
  }
  
  /**
   * Labels variable and provides metadata.
   */
  override def makeScalar(scalar:Scalar): String = {
    makeLabel(scalar) + makeAttributes(scalar)
  }
  
  /**
   * prevents labeling of samples, just looks at inner variables.
   */
  override def makeSample(sample: Sample): String = {
    sample.getVariables.map(varToString(_)).mkString("","",indent(count-indentSize)+"}\n")
  }
  
  /**
   * Make a label for the tuple before considering inner variables.
   */
  override def makeTuple(tuple: Tuple): String = {
    val label = makeLabel(tuple)
    val s = tuple.getVariables.map(varToString(_))
    count-=indentSize
    label + s.mkString("","",indent(count)+"}\n")
  }
  
  override def makeFooter(dataset: Dataset): String = "}"

  var count = indentSize

  /**
   * Used with a counter to create the proper indentation for each line of output.
   */
  def indent(num: Int): String = {
    val sb = new StringBuilder()
    for(a <- 1 to num) sb append " "
    sb.toString
  }

  /**
   * Makes a label for a given variable.
   */
  def makeLabel(variable: Variable): String ={
    count +=indentSize
    indent(count-indentSize) + variable.getName + "{\n"
  }
}
