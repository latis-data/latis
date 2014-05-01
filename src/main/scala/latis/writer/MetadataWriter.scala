package latis.writer

import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable

/**
 * Write a Dataset's Metadata as JSON.
 */
class MetadataWriter extends JsonWriter {
  
  /**
   * Override to write only domain and range info, not all samples.
   */
  override def writeFunction(function: Function) {
    printWriter.print(varToString(Sample(function.getDomain, function.getRange)))
  }
  
  /**
   * Recursively write metadata.
   * Assume only scalars have metadata, for now.
   */
  override def varToString(variable: Variable): String = {
    val sb = new StringBuilder()
    
    variable match {
      case s: Scalar => {
        sb append makeLabel(variable)
        sb append makeMetadata(variable)
      }
      case Tuple(vars) => sb append vars.map(varToString(_)).mkString(",\n")
      case f: Function => sb append varToString(Sample(f.getDomain, f.getRange))
    }
    
    sb.toString
  }
  
  /**
   * Build a String representation of the Metadata for a given Variable.
   */
  private def makeMetadata(variable: Variable): String = {
    val props = variable.getMetadata.getProperties
    if (props.nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name")) //don't include name (redundant)
        yield "\"" + name + "\": \"" + escape(value) + "\""
      ss.mkString("{\n", ",\n", "\n}")
    }
    else ""
  }
  
}