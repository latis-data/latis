package latis.writer

import java.io.OutputStream
import java.io.DataOutputStream
import latis.dm._
import java.nio.ByteOrder
import java.nio.ByteBuffer
import java.nio.charset.Charset
import latis.util.StringUtils

/**
 * 
 */
class BinaryWriter extends Writer {

  //TODO: support byte order property, default to big-endian (network, java default)
  private val order = ByteOrder.BIG_ENDIAN
  //private val order = ByteOrder.LITTLE_ENDIAN

  lazy val charset = Charset.forName("ISO-8859-1")

  private lazy val writer = new DataOutputStream(getOutputStream)
  
  def write(dataset: Dataset) {
    writeVariable(dataset.unwrap)
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
  def varToBytes(variable: Variable): Array[Byte] = variable match {
    case function: Function => {
      (for(s <- function.iterator) yield varToBytes(s)).foldLeft(Array[Byte]())(_ ++ _)
    }
    case t: Tuple => (for(v <- t.getVariables) yield varToBytes(v)).foldLeft(Array[Byte]())(_ ++ _)
    case _ => {
      val bb = ByteBuffer.allocate(variable.getSize) //potentially bigger than what we write (e.g. Index)
      //set the byte order
      bb.order(order)
      buildVariable(variable, bb)
      val bytes = new Array[Byte](bb.position())
      bb.rewind.asInstanceOf[ByteBuffer].get(bytes)
      bytes
    }
  }
 
  def buildVariable(variable: Variable, bb: ByteBuffer): ByteBuffer = variable match {
    case   scalar: Scalar   => buildScalar(scalar, bb)
    case   sample: Sample   => buildSample(sample, bb)
    case    tuple: Tuple    => buildTuple(tuple, bb)
    case function: Function => buildFunction(function, bb)
  }
  
  def buildScalar(scalar: Scalar, bb: ByteBuffer): ByteBuffer = scalar match {
    case _: Index   => bb //don't write index
    case Integer(l) => bb.putLong(l)
    case Real(d)    => bb.putDouble(d)
    case Text(s)    => {
      for(c <- StringUtils.padOrTruncate(s, scalar.getSize/2)) bb.putChar(c) //TODO: enforce charset? Is this using default charset?
      bb//.put(charset.encode(StringUtils.padOrTruncate(s, scalar.getSize/2)).rewind.asInstanceOf[ByteBuffer]) //??? //TODO: see JdbcAdapter
    }
    case Binary(b)  => bb.put(b)
  }
  
  def buildSample(sample: Sample, bb: ByteBuffer): ByteBuffer = {
    buildTuple(sample, bb)
  }
  
  def buildTuple(tuple: Tuple, bb: ByteBuffer): ByteBuffer = {
    for (v <- tuple.getVariables) buildVariable(v, bb)
    bb
  }

  //TODO: consider DataUtils
  def buildFunction(function: Function, bb: ByteBuffer): ByteBuffer = {
    for(s <- function.iterator) buildVariable(s,bb) //nested function
    bb
  }
}


object BinaryWriter {
  
  def apply(out: OutputStream): BinaryWriter = {
    val writer = new BinaryWriter()
    writer.setOutputStream(out)
    writer
  }
  
  def apply(): BinaryWriter = BinaryWriter(System.out)
}
