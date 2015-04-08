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
    
  /**
   * Construct a Binary variable from a ByteBuffer.
   * Make sure it has been flipped in case the client was lazy.
   * Update the length metadata assuming that the 8 byte
   * termination marker (DataUtils.nullMark) is present.
   */
  def apply(buffer: ByteBuffer): Binary = {
    //If the position is not 0, assume that the buffer hasn't had its limit set.
    //Set the limit and rewind.
    if (buffer.position > 0) buffer.flip
    
    //define length in metadata
    //Note: assume bytes already have termination marker
    //TODO: check before assuming, but further slow down
    val length = buffer.capacity - 8 //length is the maximum number of valid bytes (not counting the termination marker)
    val md = Metadata(Map("length" -> length.toString))
    
    new AbstractScalar(md, Data(buffer)) with Binary
  }
  
  /**
   * Construct a Binary variable from an array of bytes.
   */
  def apply(bytes: Array[Byte]): Binary = Binary(ByteBuffer.wrap(bytes))
  
  /**
   * Construct a Binary variable from a String.
   */
  def apply(string: String): Binary = Binary(string.getBytes)
  
  /**
   * Construct a Binary variable with no data.
   * Used as a template during Dataset construction.
   */
  def apply(md: Metadata): Binary = new AbstractScalar(md) with Binary
  
  /**
   * Construct a Binary variable with Metadata and Data.
   * This will trust whatever length is defined in the Metadata
   * so the user has the ability to break this, but also the flexibility
   * on whether to include the byte array termination marker (DataUtils.nullMark).
   */
  def apply(md: Metadata, data: Data): Binary = {
    new AbstractScalar(md, data) with Binary
  }
    
  /**
   * Construct a Binary variable from a ByteBuffer with Metadata.
   * Make sure it has been flipped in case the client was lazy.
   * This will trust whatever length is defined in the Metadata
   * so the user has the ability to break this, but also the flexibility
   * on whether to include the byte array termination marker (DataUtils.nullMark).
   */
  def apply(md: Metadata, buffer: ByteBuffer): Binary = {
    //If the position is not 0, assume that the buffer hasn't had its limit set.
    //Set the limit and rewind.
    if (buffer.position > 0) buffer.flip
    
    new AbstractScalar(md, Data(buffer)) with Binary
  }
  
  /**
   * Expose the data represented by this Variable as an array of bytes.
   */
  def unapply(b: Binary): Option[Array[Byte]] = {
    val bb = b.getData.getByteBuffer
    bb.rewind //make sure we are starting at the beginning
    val bytes = bb.array
    //copy to protect immutability, potential performance hit
    Some(bytes.clone)
  }
}