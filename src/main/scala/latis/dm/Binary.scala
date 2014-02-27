package latis.dm

import latis.data.Data
import latis.data.value.DoubleValue
import java.nio.Buffer
import latis.metadata.Metadata

/**
 * Binary blob.
 */
trait Binary extends Scalar

object Binary {
  
  def apply(buffer: Buffer) = {
    val size = buffer.limit
    val md = Metadata(Map("size" -> size.toString))
    new AbstractScalar(md, Data(buffer)) with Binary
  }
  
  def apply(md: Metadata): Binary = new AbstractScalar(md) with Binary
  
  def apply(md: Metadata, data: Data): Binary = new AbstractScalar(md, data) with Binary
  
  def apply(md: Metadata, buffer: Buffer): Binary = {
    //add size if not already defined
    val md2 = md.get("size") match {
      case Some(_) => md
      case None => Metadata(md.getProperties ++ Map("size" -> buffer.limit.toString))
    }
    new AbstractScalar(md2, Data(buffer)) with Binary
  }
  
//  def apply(v: Any) = v match {
//    case d: Double => new AbstractVariable(data = Data(Data(d).getByteBuffer)) with Binary
//  }
  
  def unapply(b: Binary): Option[Array[Byte]] = Some(b.getData.getByteBuffer.array)
}