package latis.writer

import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.time.Time
import latis.dm.Dataset

/**
 * Write a Dataset's Metadata as JSON.
 */
class MetadataWriter extends JsonWriter {
  
  override def makeHeader(dataset: Dataset) = {
    //Hack in the dataset name. We lost this when treating dataset as a Tuple in JsonWriter
    //"{\"" + dataset.getName + "\": "
    
    val sb = new StringBuffer()
    
    //add usual json header ({)
    sb append super.makeHeader(dataset)

    //assume single top level function
    val function = dataset.findFunction.get
    
    //Create the metadata content
    sb append "\"metadata\": {" //metadata object label
    sb append mdvarToString(Sample(function.getDomain, function.getRange)) //metadata
    sb append "}\n"
    
    sb.toString
  }
  
  /**
   * Write only the header and footer. No need to look at variables.
   */
  override def write(dataset: Dataset) {
    writeHeader(dataset)
    //dataset.getVariables.map(writeVariable(_))     
    writeFooter(dataset)
    printWriter.flush()
  }
  
  /**
   * Recursively write metadata.
   * Assume only scalars have metadata, for now.
   */
  def mdvarToString(variable: Variable): String = {
    val sb = new StringBuilder()
    
    variable match {
      case s: Scalar => {
        sb append super.makeLabel(variable)
        sb append makeMetadata(variable)
      }
      case Tuple(vars) => sb append vars.map(mdvarToString(_)).mkString(",\n")
      case f: Function => sb append mdvarToString(Sample(f.getDomain, f.getRange))
    }
    
    sb.toString
  }
  
  /**
   * Build a String representation of the Metadata for a given Variable.
   */
  private def makeMetadata(variable: Variable): String = {
    var props = variable.getMetadata.getProperties
    
    //change time units in metadata since this Writer always converts time to Java time
    if (variable.isInstanceOf[Time]) props = props + ("units" -> "milliseconds since 1970-01-01")
    
    if (props.nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name")) //don't include name (redundant)
        yield "\"" + name + "\": " + format(value)
      ss.mkString("{\n", ",\n", "\n}")
    }
    else ""
  }
  
  /**
   * Hack to allow encoding complex metadata as json in addition to escaping quotes within values.
   */
  private def format(value: String): String = {
    if (value.startsWith("{") || value.startsWith("[")) value
    else "\"" + escape(value) + "\"" //put in quotes and escape inner quotes so these will be value json values
  }
  
}