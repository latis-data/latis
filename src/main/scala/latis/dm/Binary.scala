package latis.dm

import latis.data.Data
import latis.metadata.Metadata
import java.nio.ByteBuffer
import latis.util.DataUtils

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
      case None => md + ("length" -> buffer.capacity.toString)
    }
    new AbstractScalar(md2, Data(buffer)) with Binary
  }
  
  /**
   * Expose the data represented by this Variable as an array of bytes.
   * Use the 'limit' of the Data's ByteBuffer to add a nullMark so we
   * know where the useful bytes end.
   */
  def unapply(b: Binary): Option[Array[Byte]] = {
    val bb = b.getData.getByteBuffer
    bb.rewind //just to make sure we are starting at the beginning
    val bytes = new Array[Byte](bb.capacity + 8) //allocate array for all bytes
    bb.get(bytes, 0, bb.limit) //get the useful bytes
    //insert null marker after useful bytes
    for (i <- 0 until 8) bytes(bb.limit + i) = DataUtils.nullMark(i)
    Some(bytes)
  }
}