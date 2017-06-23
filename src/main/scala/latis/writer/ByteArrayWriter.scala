package latis.writer

import java.io.ByteArrayOutputStream
import java.io.OutputStream

import latis.dm._

/**
 * Write data as binary to an in memory array of Bytes.
 */
class ByteArrayWriter extends BinaryWriter {

  /**
   * Override to always write to a ByteArrayOutputStream.
   */
  override def getOutputStream: OutputStream = _out
  private val _out = new ByteArrayOutputStream()
  
  /**
   * Provide access to the array of Bytes (after writing the Dataset).
   */
  def getBytes: Array[Byte] = _out.toByteArray()
  
}


object ByteArrayWriter {
  
  def apply(): ByteArrayWriter = new ByteArrayWriter
  
  /**
   * Convenience method to get the array of bytes from a dataset.
   */
  def getBytes(dataset: Dataset): Array[Byte] = {
    val writer = ByteArrayWriter()
    writer.write(dataset)
    writer.getBytes
  }
}
