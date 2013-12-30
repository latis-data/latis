package latis.dm

import latis.data.value.StringValue
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.TextData
import latis.data.Data
import latis.data.seq.StringSeqData

trait Text extends Scalar { //[String] { 
  
  //Get the nominal/max length of this Text Variable.
  //If not defined in the metadata, the default is 4 chars (8 bytes).
  //This is NOT necessarily the actual length of the encapsulated String.
  //TODO: rename to avoid confusion with Variable.getLength?
  def length: Int = {
    getMetadata.get("length") match {
      case Some(l) => l.toInt
      case None => 4
    }
  }
  
  //Note: stripping off any white space padding
  //TODO: need to be able to preserve text with white space padding?
  //  use other symbol for padding?
  def value: String = getData.asInstanceOf[TextData].stringValue.trim
  
  //Ordered method.
  override def compare(that: String): Int = value compare that
  
}

object Text {
  
  def apply(v: String): Text = new Variable2(data = Data(v)) with Text
  
  //def apply(name: String, v: String): Text = new Variable2(Metadata(name), Data(v)) with Text

  def apply(md: Metadata): Text = new Variable2(md) with Text
  
  def apply(md: Metadata, v: String): Text = new Variable2(md, Data(v)) with Text
  
  def apply(md: Metadata, vs: Seq[String]): Text = {
    val data = new StringSeqData(vs.toIndexedSeq, md("length").toInt) 
    new Variable2(md, data) with Text
  }

  def unapply(v: Text): Option[String] = Some(v.value)
}