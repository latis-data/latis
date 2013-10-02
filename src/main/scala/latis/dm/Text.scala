package latis.dm

import latis.data.value._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.TextData
import latis.data.Data
import latis.data.seq.StringSeqData

class Text extends Scalar {
  //TODO: accept only Text Data
  
  //Get the nominal/max length of this Text Variable.
  //If not defined in the metadata, the default is 4 chars (8 bytes).
  //This is NOT the actual length of the encapsulated String
  def length: Int = {
    metadata.get("length") match {
      case Some(l) => l.toInt
      case None => 4
    }
  }
  
  //Note: stripping off any white space padding
  def stringValue: String = data.asInstanceOf[TextData].stringValue.trim
  
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
  
  def apply(md: Metadata, v: String): Text = {
    val t = new Text
    t._metadata = md
    t._data = StringValue(v)
    t
  }
    
  def apply(md: Metadata, vs: Seq[String]): Text = {
    val t = new Text
    t._metadata = md
    t._data = new StringSeqData(vs.toIndexedSeq, md("length").toInt) 
    t
  }
  
  def unapply(v: Text): Option[String] = Some(v.stringValue)
}