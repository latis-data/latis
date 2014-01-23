package latis.dm

import latis.data.Data
import latis.data.value.DoubleValue
import java.nio.Buffer
import latis.metadata.Metadata

/**
 * Binary blob.
 */
trait Binary extends Scalar {
  
  def length: Int = {
    getMetadata.get("length") match {
      case Some(l) => l.toInt
      case None => throw new Error("Must declare length of Binary Variable.")
    }
  }
  
}

object Binary {
  
//TODO: set length if not already set (see Text)
  
  def apply(buffer: Buffer) = {
    val size = buffer.limit
    val md = Metadata(Map("length" -> size.toString))
    new AbstractScalar(md, Data(buffer)) with Binary
  }
  
  def apply(md: Metadata): Binary = new AbstractScalar(md) with Binary
  
  def apply(md: Metadata, buffer: Buffer): Binary = {
    //add length if not already defined
    val md2 = md.get("length") match {
      case Some(_) => md
      case None => Metadata(md.getProperties ++ Map("length" -> buffer.limit.toString))
    }
    new AbstractScalar(md2, Data(buffer)) with Binary
  }
  
//  def apply(v: Any) = v match {
//    case d: Double => new AbstractVariable(data = Data(Data(d).getByteBuffer)) with Binary
//  }
  
  def unapply(b: Binary): Option[Array[Byte]] = Some(b.getData.getByteBuffer.array)
}