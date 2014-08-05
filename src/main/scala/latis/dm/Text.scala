package latis.dm

import latis.data.Data
import latis.metadata.Metadata

/**
 * Trait for Scalars representing text data.
 */
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
  
  def stringValue = getValue.asInstanceOf[String]
  
  override def compare(that: Scalar): Int = that match {
    case Text(s) => stringValue compare s
    case _ => throw new Error("Can't compare " + this + " with " + that)
  }
  
  //TODO: getStringValue? akin to Number.doubleValue, stringValue for all Vars?
}

object Text {
  
  val DEFAULT_LENGTH = 4
  
  def apply(v: String): Text = new AbstractScalar(data = Data(v)) with Text

  def apply(md: Metadata): Text = new AbstractScalar(md) with Text
  
  def apply(md: Metadata, data: Data): Text = new AbstractScalar(md, data) with Text
  
  def apply(md: Metadata, v: String): Text = new AbstractScalar(md, Data(v)) with Text

  def unapply(v: Text): Option[String] = Some(v.getValue.asInstanceOf[String])
}