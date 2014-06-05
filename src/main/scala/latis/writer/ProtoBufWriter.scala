package latis.writer

import java.nio.ByteBuffer
import latis.dm._
import java.nio.ByteOrder
import scala.math._
import java.io.DataOutputStream

class ProtoBufWriter extends BinaryWriter {
  
  private lazy val writer = new DataOutputStream(getOutputStream)
  
  override def writeVariable(variable: Variable) = writer.write(varToBytes(variable))
  
  override def varToBytes(variable: Variable): Array[Byte] = {
    val bb = ByteBuffer.allocate(getBufSize(variable))
    bb.order(ByteOrder.BIG_ENDIAN)
    buildVariable(variable, bb)
    val bytes = new Array[Byte](bb.position())
    bb.flip.asInstanceOf[ByteBuffer].get(bytes)
    bytes
  }
  
  override def buildScalar(scalar: Scalar, bb: ByteBuffer): ByteBuffer = {
    tag += 1
    scalar match {
      case _: Index   => bb
      case Integer(l) => buildVarint(l, buildKey(tag, 0, bb))
      case Real(d)    => buildKey(tag, 1, bb).putDouble(d)
      case Text(s)    => buildVarint(s.length, buildKey(tag, 2, bb)).put(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      case Binary(b)  => buildKey(tag, 2, bb).put(b)
    }
  }
  
  override def buildSample(sample: Sample, bb: ByteBuffer): ByteBuffer = {
    val temp = tag
    tag = 0
    sample match {
      case Sample(d, r: Tuple) => {
        buildVariable(d, bb)
        for(v <- r.getVariables) buildVariable(v, bb)
      }
      case _ => for(v <- sample.getVariables) buildVariable(v, bb)
    }
    tag = temp
    bb
  }
  
  override def buildTuple(tuple: Tuple, bb: ByteBuffer): ByteBuffer = {
    val temp = tag
    tag = 0
    val aa = ByteBuffer.allocate(getBufSize(tuple))
    for(v <- tuple.getVariables) buildVariable(v, aa)
    tag = temp + 1
    buildVarint(aa.position, buildKey(tag, 2, bb))
    aa.flip
    while(aa.hasRemaining) bb.put(aa.get)
    bb
  }
  
  override def buildFunction(function: Function, bb: ByteBuffer): ByteBuffer = {
    val temp = tag
    tag = 0
    val aa = ByteBuffer.allocate(bb.capacity())
    for(sample <- function.iterator) buildVariable(sample, aa)
    tag = temp + 1
    buildVarint(aa.position, buildKey(tag, 2, bb))
    aa.flip
    while(aa.hasRemaining) bb.put(aa.get)
    bb
  }
  
  def buildVarint(long: Long, bb: ByteBuffer) = {
    var n = long
    while((n & (~0x00<<7))!=0x00) {
      bb.put(((n & 0x7F) | 0x80).toByte)
      n = n>>>7
    }
    bb.put((n & 0x7f).toByte)
  }
  
  def buildKey(tag: Int, wtype: Int, bb: ByteBuffer) = {
    buildVarint(((tag << 3) | wtype), bb)
  }
  
  def getBufSize(variable: Variable): Int = {
    variable match {
      case _: Index => 0
      case r: Real => 8 + keySize
      case i: Integer => 10 + keySize
      case Text(t) => t.length + getBufSize(Integer(t.length)) + keySize
      case _: Binary => variable.getMetadata("size") match {
        case Some(l) => l.toInt + getBufSize(Integer(l.toInt)) + keySize
        case None => throw new Error("Must declare length of Binary Variable.")
      }
      case Tuple(vars) => vars.map(getBufSize(_)).sum + getBufSize(Integer(vars.map(getBufSize(_)).sum)) + keySize
      case f: Function => {
        f.getLength * (getBufSize(f.getDomain) + getBufSize(f.getRange)) + getBufSize( Integer(f.getLength * (getBufSize(f.getDomain) + getBufSize(f.getRange))) ) + keySize
      }
    }
  }
  def keySize = if(tag<16) 1 else (2 + tag/2048)

  var tag = 0
  
}