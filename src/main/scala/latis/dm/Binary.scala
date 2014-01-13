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
  
  def apply(buffer: Buffer) = new Variable2(data = Data(buffer)) with Binary
  
  def apply(md: Metadata): Binary = new AbstractScalar(md) with Binary
  
//  def apply(v: Any) = v match {
//    case d: Double => new Variable2(data = Data(Data(d).getByteBuffer)) with Binary
//  }
  
  def unapply(b: Binary): Option[Buffer] = Some(b.getData.getByteBuffer)
}