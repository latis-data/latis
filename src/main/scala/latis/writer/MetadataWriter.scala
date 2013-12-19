package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class MetadataWriter extends JsonWriter {
    
  //override to write only domain and range info, not all samples
  override def writeFunction(function: Function) {
    printWriter.print(varToString(Sample(function.getDomain, function.getRange)))
  }
  
  //assume only scalars have metadata, for now
  override def varToString(variable: Variable): String = {
    val sb = new StringBuilder()
    sb append makeLabel(variable)
    sb append makeMetadata(variable)
    
    variable match {
      case Tuple(vars) => sb append vars.map(varToString(_)).mkString(",\n")
      case Function(d,r) => sb append varToString(Sample(d,r))
      case _ => //nothing to add for scalars
    }
    
    sb.toString
  }
  
  def makeMetadata(variable: Variable): String = {
    val props = variable.getMetadata.getProperties
    if (props.nonEmpty) {
      val ss = for ((name, value) <- props) 
        yield "\"" + name + "\": \"" + escape(value) + "\""
      ss.mkString("{\n", ",\n", "\n}")
    }
    else ""
  }
  
}