package latis.dm

import latis.data._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata

//class Text(metadata: Metadata, data: Data) extends Scalar(metadata, data) {
class Text extends Scalar {
  //TODO: accept only Text Data
  
  //TODO: use get Option
  def compare(that: Double): Int = data.doubleValue.compareTo(that) //TODO: error comparing Text to double?
    
  def compare(that: String): Int = data.stringValue.compareTo(that)
  
}

object Text {
  
  def apply(v: String): Text = {
    val t = new Text
    t._data = StringValue(v)
    t
  }
  
  def apply(name: String, v: String): Text = {
    val t = new Text
    t._metadata = Metadata(name)
    t._data = StringValue(v)
    t
  }
  
  def unapply(v: Text) = Some(v.data.toString)
}