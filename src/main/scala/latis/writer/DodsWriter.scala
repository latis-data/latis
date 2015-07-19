package latis.writer

import latis.dm._
import java.nio.ByteBuffer
import java.io.DataOutputStream
import java.io.PrintWriter

/**
 * Combines the DasWriter output with binary data.
 */
class DodsWriter extends BinaryWriter {
  
  private[this] lazy val writer = new DataOutputStream(getOutputStream)

  /**
   * Each sample of a function begins with these bytes.
   */
  final val START_OF_INSTANCE: Array[Byte] = Array(0x5A.toByte, 0, 0, 0)

  /**
   * Each function ends with these bytes.
   */
  final val END_OF_SEQUENCE: Array[Byte] = Array(0xA5.toByte, 0, 0, 0)

  override def write(dataset: Dataset) {
    writeHeader(dataset)
    writeVariable(dataset.unwrap)
    writer.flush()
  }
  
  /**
   * Header contains the DasWriter output of the dataset.
   */
  def writeHeader(dataset: Dataset) = {
    val w = new DdsWriter()
    val s = w.makeHeader(dataset) + w.varToString(dataset.unwrap).mkString("") + w.makeFooter(dataset) + "\nData:\n"
    writer.write(s.getBytes)    
  }

  /**
   * Includes the START_OF_INSTANCE and END_OF_SEQUENCE bytes in the data.
   */
  override def writeVariable(variable: Variable) = variable match {
    case f: Function => {
      for (sample <- f.iterator){
        writer.write(START_OF_INSTANCE)
        writer.write(varToBytes(sample))
      }
      writer.write(END_OF_SEQUENCE)
    }
    case _ => {
      writer.write("\n".getBytes)
      writer.write(varToBytes(variable))
    }
  }

  /**
   * Bypasses use of ByteBuffer by writing directly to the output stream.
   */
  override def buildFunction(function: Function, bb: ByteBuffer): ByteBuffer = {
     writeVariable(function)
     bb
  }

}