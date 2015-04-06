package latis.dm

import latis.data.Data
import latis.metadata.Metadata

import java.nio.ByteBuffer

/**
 * A single variable (Scalar) that represents an arbitrary binary 'blob'.
 */
trait Binary extends Scalar

object Binary {
  
  def apply(buffer: ByteBuffer) = {
    //TODO: see if it needs to be flipped?
    val size = buffer.capacity
    val md = Metadata(Map("length" -> size.toString))
    new AbstractScalar(md, Data(buffer)) with Binary
  }
  
  def apply(md: Metadata): Binary = new AbstractScalar(md) with Binary
  
  def apply(md: Metadata, data: Data): Binary = new AbstractScalar(md, data) with Binary
  
  def apply(md: Metadata, buffer: ByteBuffer): Binary = {
    //add length if not already defined
    val md2 = md.get("length") match {
      case Some(_) => md
      case None => Metadata(md.getProperties ++ Map("length" -> buffer.capacity.toString))
    }
    new AbstractScalar(md2, Data(buffer)) with Binary
  }
  
  /**
   * Expose the data represented by this Variable as an array of bytes.
   */
  def unapply(b: Binary): Option[Array[Byte]] = Some(b.getData.getBytes)
}