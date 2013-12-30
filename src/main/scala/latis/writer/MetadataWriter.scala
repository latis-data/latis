package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class MetadataWriter extends JsonWriter {
  //TODO: expose the metadata as a Dataset so we can count on parent's logic  
  
  //override to write only domain and range info, not all samples
  override def writeFunction(function: Function) {
    printWriter.print(varToString(Sample(function.getDomain, function.getRange)))
  }
  
  //assume only scalars have metadata, for now
  override def varToString(variable: Variable): String = {
    val sb = new StringBuilder()
    
    variable match {
      case s: Scalar => {
        sb append makeLabel(variable)
        sb append makeMetadata(variable)
      }
      case Tuple(vars) => sb append vars.map(varToString(_)).mkString(",\n")
      case Function(d,r) => sb append varToString(Sample(d,r))
    }
    
    sb.toString
  }
  
  def makeMetadata(variable: Variable): String = {
    val props = variable.getMetadata.getProperties
    if (props.nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name")) //don't include name (redundant)
        yield "\"" + name + "\": \"" + escape(value) + "\""
      ss.mkString("{\n", ",\n", "\n}")
    }
    else ""
  }
  
}