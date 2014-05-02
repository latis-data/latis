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
    val size = buffer.limit
    val md = Metadata(Map("size" -> size.toString))
    new AbstractScalar(md, Data(buffer)) with Binary
  }
  
  def apply(md: Metadata): Binary = new AbstractScalar(md) with Binary
  
  def apply(md: Metadata, data: Data): Binary = new AbstractScalar(md, data) with Binary
  
  def apply(md: Metadata, buffer: ByteBuffer): Binary = {
    //add size if not already defined
    val md2 = md.get("size") match {
      case Some(_) => md
      case None => Metadata(md.getProperties ++ Map("size" -> buffer.limit.toString))
    }
    new AbstractScalar(md2, Data(buffer)) with Binary
  }
  
  /**
   * Expose the data represented by this Variable as an array of bytes.
   */
  def unapply(b: Binary): Option[Array[Byte]] = Some(b.getData.getBytes)
}