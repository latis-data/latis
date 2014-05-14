package latis.writer

import latis.dm.Variable
import latis.dm.Scalar
import latis.time.Time
import latis.dm.Sample
import latis.dm.Index
import latis.dm.Tuple
import latis.dm.Function
import latis.dm.Dataset

class JsonMetadataAndDataWriter extends JsonWriter {
  //TODO: refactor to reuse these pieces from Metadata and CompactJson Writers
  
  /**
   * Override to insert metadata.
   */
  override def makeHeader(dataset: Dataset) = {
    val sb = new StringBuffer(super.makeHeader(dataset))
    
    //assume single top level function
    val function = dataset.findFunction.get
    sb append "\"metadata\": {" //metadata object label
    sb append mdvarToString(Sample(function.getDomain, function.getRange)) //metadata
    sb append "},\n"
    sb append "\"data\": " //start data object
    
    sb.toString
  }
  
  //=== below mod'd from MetadataWriter
  
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
    val props = variable.getMetadata.getProperties
    if (props.nonEmpty) {
      val ss = for ((name, value) <- props.filterNot(_._1 == "name")) //don't include name (redundant)
        yield "\"" + name + "\": \"" + escape(value) + "\""
      ss.mkString("{\n", ",\n", "\n}")
    }
    else ""
  }
  
  //==== everything below from CompactJsonWriter which does the data part
  
  override def makeLabel(variable: Variable): String = ""
    
  /**
   * Override to present time in native JavaScript units: milliseconds since 1970.
   */
  override def makeScalar(scalar: Scalar): String = scalar match {
    case t: Time => t.getJavaTime.toString  //use java time for "compact" json
    case _ => super.makeScalar(scalar)
  }
  
  override def makeSample(sample: Sample): String = {
    val Sample(d, r) = sample
    d match {
      case _: Index => varToString(r) //drop Index domain
      case _ => varToString(Tuple(d.toSeq ++ r.toSeq)) //combine domain and range vars into one Tuple
      //TODO: not tested for nested variables
    }
  }
    
  /**
   * Represent a tuple as an array with each element being an array of values.
   */
  override def makeTuple(tuple: Tuple): String = {
    tuple.getVariables.map(varToString(_)).mkString("[", ",", "]") 
  }
}