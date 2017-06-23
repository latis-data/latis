package latis.writer

import java.nio.ByteBuffer
import latis.dm._
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.io.DataOutputStream
import scala.collection.mutable.ArrayBuilder

/**
 * Writes the binary representation of the dataset corresponding to 
 * the .proto file produced by the ProtoWriter.
 */
class ProtoBufWriter extends BinaryWriter {
  
  private lazy val writer = new DataOutputStream(getOutputStream)
  
  override lazy val charset = Charset.forName("utf-8")
  
  override def writeVariable(variable: Variable): Unit = writer.write(varToBytes(variable))
  
  /**
   * Each variable type is handled in a different way
   */
  override def varToBytes(variable: Variable): Array[Byte] = variable match {
    case f: Function => makeFunction(f)
    case s: Sample => makeSample(s)
    case t: Tuple => makeTuple(t)
    case s: Scalar => makeScalar(s)
  }
  
  /**
   * Iterate through samples to get bytes
   */
  def makeFunction(function: Function): Array[Byte] = {
    val temp = tag
    tag = 0
    val bytes = function.iterator.map(varToBytes(_)).foldLeft(Array[Byte]())(_ ++_)
    val len = bytes.length
    tag = temp + 1
    makeKey(tag, 2) ++ makeVarint(len) ++ bytes
  }
  
  /**
   * Much the same a MakeTuple, but does not add a key and length
   */
  def makeSample(sample: Sample): Array[Byte] = {
    val temp = tag
    tag = 0
    val bytes = sample.getVariables.map(varToBytes(_)).toList
    val len = bytes.map(_.length).sum
    tag = temp
    bytes.foldLeft(Array[Byte]())(_ ++ _)
  }
  
  /**
   * Get bytes for all of the tuple's variables, and prepend with key and length
   */
  def makeTuple(tuple: Tuple): Array[Byte] = {
    val temp = tag
    tag = 0
    val bytes = tuple.getVariables.map(varToBytes(_)).toList
    val len = bytes.map(_.length).sum
    tag = temp + 1
    bytes.foldLeft(makeKey(tag, 2) ++ makeVarint(len))(_ ++ _)
  }
  
  /**
   * Encode a scalar depending on type. 
   */
  def makeScalar(scalar: Scalar): Array[Byte] = {
    tag += 1
    scalar match {
      case _: Index => Array[Byte]()
      case Integer(l) => makeKey(tag, 0) ++ makeVarint(l)
      case Real(d) => makeKey(tag, 1) ++ ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(d).array.take(8)
      case Text(s) => makeKey(tag, 2) ++ makeVarint(s.length) ++ charset.encode(s).rewind.asInstanceOf[ByteBuffer].array.take(s.length)
      case Binary(b) => makeKey(tag, 2) ++ b
    }
  }
  
  /**
   * Get the varint representation of a number as an array of bytes
   */
  def makeVarint(long: Long): Array[Byte] = {
    var n = long
    val ab = new ArrayBuilder.ofByte
    while((n & (~0x00<<7))!=0x00) {
      ab += (((n & 0x7F) | 0x80).toByte)
      n = n>>>7
    }
    ab += ((n & 0x7f).toByte)
    ab.result
  }
  
  /**
   * Make a key given a tag and a type
   */
  def makeKey(tag: Int, wtype: Int): Array[Byte] = {
    makeVarint(((tag << 3) | wtype))
  }
  
  var tag = 0
  
}
