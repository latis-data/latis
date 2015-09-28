package latis.dm

import latis.data.Data
import latis.data.value.StringValue
import latis.metadata.Metadata
import scala.collection.immutable.StringOps

/**
 * Trait for Scalars representing text data.
 */
trait Text extends Scalar with Ordering[Text] {
  
  /**
   * Implement Ordering for Real Variables so we can sort.
   */
  def compare(x: Text, y: Text): Int = (x,y) match {
    case (Text(a), Text(b)) => a compare b
  }
  
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

}

object Text {
  
  val DEFAULT_LENGTH = 4
  //TODO: parameterize in latis.properties?
  
  def apply(data: Data): Text = data match {
    case v: StringValue => new AbstractScalar(data = data) with Text
    case _ => throw new UnsupportedOperationException("A Text must be constructed with a StringValue.")
  }
  
  //def apply(v: String): Text = Text(StringValue(v))
  def apply(v: Any): Text = Text(StringValue(v.toString))

  
  def apply(md: Metadata, data: Data): Text = data match {
    case v: StringValue => new AbstractScalar(md, data) with Text
    case _ => throw new UnsupportedOperationException("A Text must be constructed with a StringValue.") 
  }
  
  def apply(md: Metadata, v: Any): Text = Text(md, StringValue(v.toString))

  def apply(md: Metadata): Text = new AbstractScalar(md) with Text


  def unapply(v: Text): Option[String] = Some(v.getValue.asInstanceOf[String]) 
  //TODO: scalar.stringValue?
}