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
    val v = dataset match {
      case Dataset(v) => v
      case _ => null
    }
    writeVariable(v)
    writer.flush()
  }
  
  /**
   * Header contains the DasWriter output of the dataset.
   */
  def writeHeader(dataset: Dataset) = {
    val w = new DdsWriter()
    val v = dataset match {
      case Dataset(v) => w.varToString(v).mkString("")
      case _ => ""
    }
    val s = w.makeHeader(dataset) + v + w.makeFooter(dataset) + "\nData:\n"
    writer.write(s.getBytes)    
  }

  override def writeVariable(variable: Variable) = {
    writer.write(varToBytes(variable))
  }
  
  /**
   * Get the bytes of a variable encoded for dods.
   */
  override def varToBytes(v: Variable): Array[Byte] = v match {
    case s: Scalar => scalarBytes(s)
    case t: Tuple => tupleBytes(t)
    case f: Function => functionBytes(f)
  }
  
  /**
   * XDR encoding, using Float64 for Reals and Int32 for Integers.
   * Each returned array must be a multiple of 4 bytes long. 
   */
  def scalarBytes(s: Scalar): Array[Byte] = s match {
    case Integer(i) => ByteBuffer.allocate(4).putInt(i.toInt).array
    case Real(d) => ByteBuffer.allocate(8).putDouble(d).array
    case Text(s) => {
      val l = s.length
      val fill = (4 - (l % 4)) % 4
      val bb = ByteBuffer.allocate(4 + l + fill)
      bb.putInt(l)
      s.foreach(c => bb.put(c.toByte))
      bb.array
    }
  }
  
  /**
   * Each sample begins with the start of instance tag.
   */
  def sampleBytes(s: Sample): Array[Byte] = START_OF_INSTANCE ++ tupleBytes(s)
  
  /**
   * Tuples just have their variables encoded sequentially.
   */
  def tupleBytes(t: Tuple): Array[Byte] = t.getVariables.flatMap(varToBytes(_)).toArray
  
  /**
   * Functions end with the end of sequence tag.
   */
  def functionBytes(f: Function): Array[Byte] = f.iterator.flatMap(sampleBytes(_)) ++: END_OF_SEQUENCE

}
