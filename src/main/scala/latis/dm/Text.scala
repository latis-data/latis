package latis.dm

import latis.data.Data
import latis.data.value.StringValue
import latis.metadata.Metadata
import scala.collection.immutable.StringOps

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
  
  //TODO: getStringValue? akin to Number.doubleValue, stringValue for all Vars?
}

object Text {
  
  val DEFAULT_LENGTH = 4
  
  def apply(v: String): Text = new AbstractScalar(data = Data(v.toString)) with Text
  def apply(v: AnyVal): Text = new AbstractScalar(data = Data(v.toString)) with Text

  def apply(md: Metadata): Text = new AbstractScalar(md) with Text
  
  def apply(md: Metadata, data: Data): Text = new AbstractScalar(md, data) with Text
  
  def apply(md: Metadata, v: Any): Text = v match {
    case sv: StringValue => new AbstractScalar(md, sv) with Text
    //not allowed  case av: AnyVal => new AbstractScalar(md, Data(v.toString)) with Text
    //TODO: consider restricting this to valid values
    case _ => new AbstractScalar(md, Data(v.toString)) with Text
  }

  def unapply(v: Text): Option[String] = Some(v.getValue.asInstanceOf[String])
}