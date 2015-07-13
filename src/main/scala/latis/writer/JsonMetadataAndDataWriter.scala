package latis.writer

import latis.dm.Variable
import latis.dm.Scalar
import latis.time.Time
import latis.dm.Sample
import latis.dm.Index
import latis.dm.Tuple
import latis.dm.Function
import latis.dm.Dataset
import scala.collection.mutable.ArrayBuffer

class JsonMetadataAndDataWriter extends JsonWriter {
  //TODO: refactor to reuse these pieces from Metadata and CompactJson Writers which are rapidly becoming obsolete
    
  /**
   * Override to insert metadata.
   */
  override def makeHeader(dataset: Dataset) = {
    //Hack in the dataset name. We lost this when treating dataset as a Tuple in JsonWriter
    //"{\"" + dataset.getName + "\": "
    
    val sb = new StringBuffer()
    
    //add usual json header ({)
    sb append super.makeHeader(dataset)

    //TODO: assumes Dataset contains a Function
    dataset match {
      case Dataset(function: Function) => {
        //Create the metadata content
        sb append "\"metadata\": {" //metadata object label
        sb append mdvarToString(Sample(function.getDomain, function.getRange)) //metadata
        sb append "},\n"
    
        sb append "\"data\": " //start data object
      }
    
      case _ => ??? //TODO: deal with Dataset that does not have a top level Function
    }
    
    sb.toString
  }
  
//  /**
//   * Override to close extra "{" that we added in the header.
//   */
//  override def makeFooter(dataset: Dataset) = {
//    super.makeFooter(dataset) + "}"
//  }
  
  //=== below mod'd from MetadataWriter
  
  /**
   * Recursively write metadata.
   * Assume only scalars have metadata, for now.
   */
  def mdvarToString(variable: Variable): String = {
    val sb = new StringBuilder()
    
    variable match {
      case s: Scalar => s match {
        case i: Index => 
        case _ => {
          sb append super.makeLabel(variable)
          sb append makeMetadata(variable)
        }
      }
      case Tuple(vars) => sb append vars.filterNot(_.isInstanceOf[Index]).map(mdvarToString(_)).mkString(",\n")
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
  
  //==== everything below from CompactJsonWriter which does the data part
  
  override def makeLabel(variable: Variable): String = ""
    
  /**
   * Override to present time in native JavaScript units: milliseconds since 1970.
   */
  override def makeScalar(scalar: Scalar): String = {
    //quick fix to replace missing data with "null"
    //TODO: user should apply replace_missing(NaN) once that is working
    if (scalar.isMissing) "null"
    else scalar match {
      case t: Time => t.getJavaTime.toString  //use java time for "compact" json
      case _ => super.makeScalar(scalar)
    }
  }
  
  
  override def makeSample(sample: Sample): String = {
    //break sample into domain and range components
    val Sample(d, r) = sample
    
    //TODO: handle more general cases with scalars and tuples in the range with the function
    //TODO: handle 'flattened' option - one row per outer sample without inner domain values
    r match {
      case f: Function => {
        d match {
          case i: Index => 
          case _ => prepend :+= varToString(d) //only the domain values are repeated, for now
        }
        val lines = f.iterator.map(makeSample(_))
        //lines.mkString("[", sys.props("line.separator"), "]") 
        val delim = "," + newLine
        val str = lines.mkString(delim) 
        prepend = prepend.dropRight(d.toSeq.length)
        str
      }
      case _ => {
        val vars = (d.toSeq ++ r.toSeq).filterNot(_.isInstanceOf[Index])
        val vs = vars.map(varToString(_))
        (prepend ++ vs).mkString("[", ",", "]") 
      }
    }
  }
    
  /**
   * Represent a tuple as an array with each element being an array of values.
   */
  override def makeTuple(tuple: Tuple): String = {
    tuple.getVariables.map(varToString(_)).mkString("[", ",", "]") 
  }
}