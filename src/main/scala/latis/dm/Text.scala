package latis.dm

import latis.data.value._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.TextData

class Text extends Scalar {
  //TODO: accept only Text Data
  
  def stringValue: String = data.asInstanceOf[TextData].stringValue
  
  //support lexical ordering
  def compare(that: String): Int = stringValue.compareTo(that)
  
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
  
  def apply(md: Metadata): Text = {
    val t = new Text
    t._metadata = md
    t
  }
  
  def unapply(v: Text): Option[String] = Some(v.stringValue)
}