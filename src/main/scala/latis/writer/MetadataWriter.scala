package latis.writer

import latis.dm._
import latis.time._
import java.io._
import scala.collection.mutable.MapBuilder
import latis.util.FirstThenOther

class MetadataWriter extends JsonWriter {
  //TODO: expose the metadata as a Dataset so we can count on parent's logic  
  
  //The Dataset may have one variable (the function) but we only care about the 
  //  set of scalars here, for now. //TODO: review for more general case.
//  override def makeHeader(dataset: Dataset) = "{\"" + dataset.getName + "\": {\n"
//  override def makeFooter(dataset: Dataset) = "}}"
  
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
      val ss = for ((name, value) <- props.filterNot(_._1 == "name")) //don't include name (redundant)
        yield "\"" + name + "\": \"" + escape(value) + "\""
      ss.mkString("{\n", ",\n", "\n}")
    }
    else ""
  }
  
}