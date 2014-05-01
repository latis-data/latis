package latis.writer

import java.io.OutputStream
import java.io.DataOutputStream
import latis.dm._
import java.nio.ByteOrder
import java.nio.ByteBuffer

/**
 * 
 */
class BinaryWriter extends Writer {

  //TODO: support byte order property, default to big-endian (network, java default)
  private val order = ByteOrder.BIG_ENDIAN
  //private val order = ByteOrder.LITTLE_ENDIAN

  private lazy val writer = new DataOutputStream(getOutputStream)
  
  def write(dataset: Dataset) {
    dataset.getVariables.map(writeVariable(_))
    writer.flush()
  }
  
  /**
   * Write top level variables.
   */
  def writeVariable(variable: Variable) = variable match {
    case Function(it) => for (sample <- it) writer.write(varToBytes(sample))
    case _ => writer.write(varToBytes(variable))
  }
  
  /**
   * Recursively build up a ByteBuffer
   */
  def varToBytes(variable: Variable): Array[Byte] = {
    val bb = ByteBuffer.allocate(variable.getSize) //potentially bigger than what we write (e.g. Index)
    //set the byte order
    bb.order(order)
    buildVariable(variable, bb)
    val bytes = new Array[Byte](bb.position())
    bb.rewind.asInstanceOf[ByteBuffer].get(bytes)
    bytes
  }
 
  def buildVariable(variable: Variable, bb: ByteBuffer): ByteBuffer = variable match {
    case   scalar: Scalar   => buildScalar(scalar, bb)
    case    tuple: Tuple    => buildTuple(tuple, bb)
    case function: Function => buildFunction(function, bb)
  }
  
  def buildScalar(scalar: Scalar, bb: ByteBuffer): ByteBuffer = scalar match {
    case _: Index   => bb //don't write index
    case Integer(l) => bb.putLong(l)
    case Real(d)    => bb.putDouble(d)
    case Text(s)    => ??? //TODO: see JdbcAdapter
    case Binary(b)  => bb.put(b)
  }
  
  def buildTuple(tuple: Tuple, bb: ByteBuffer): ByteBuffer = {
    for (v <- tuple.getVariables) buildVariable(v, bb)
    bb
  }
  
  def buildFunction(function: Function, bb: ByteBuffer): ByteBuffer = ??? //TODO: nested function
}


object BinaryWriter {
  
  def apply(out: OutputStream): BinaryWriter = {
    val writer = new BinaryWriter()
    writer.setOutputStream(out)
    writer
  }
  
  def apply(): BinaryWriter = BinaryWriter(System.out)
}