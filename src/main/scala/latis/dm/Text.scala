package latis.dm

import latis.data.value.StringValue
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.TextData
import latis.data.Data
import latis.data.seq.StringSeqData

trait Text extends Scalar { 
  
  //Get the nominal/max length of this Text Variable.
  //If not defined in the metadata, the default is 4 chars (8 bytes).
  //This is NOT necessarily the actual length of the encapsulated String.
  //TODO: rename to avoid confusion with Variable.getLength?
  def length: Int = {
    getMetadata.get("length") match {
      case Some(l) => l.toInt
      case None => Text.DEFAULT_LENGTH
    }
  }
  
  //TODO: getStringValue? akin to Number.doubleValue, stringValue for all Vars?
}

object Text {
  
  val DEFAULT_LENGTH = 4
  
  def apply(v: String): Text = new AbstractScalar(data = Data(v)) with Text
  
  //def apply(name: String, v: String): Text = new AbstractVariable(Metadata(name), Data(v)) with Text

  def apply(md: Metadata): Text = new AbstractScalar(md) with Text
  
  def apply(md: Metadata, data: Data): Text = new AbstractScalar(md, data) with Text
  
  def apply(md: Metadata, v: String): Text = new AbstractScalar(md, Data(v)) with Text
  
  def apply(md: Metadata, vs: Seq[String]): Text = {
    val data = new StringSeqData(vs.toIndexedSeq, md("length").toInt) 
    new AbstractScalar(md, data) with Text
  }

  def unapply(v: Text): Option[String] = Some(v.getValue.asInstanceOf[String])
}