package latis.reader.tsml

import java.nio.ByteBuffer
import latis.reader.tsml.ml.Tsml
import latis.data.Data
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text
import latis.dm.Variable
import scala.io.Source
import latis.util.StringUtils

class ProtoBufAdapter(tsml: Tsml) extends IterativeAdapter[Array[ByteBuffer]](tsml) {
  
  private var source: Source = null
  
  def getDataSource: Source = {
    if (source == null) source = Source.fromURL(getUrl())
    source
  }
  
  override def close {
    if (source != null) source.close
  }
  
  //going to assume that the dataset has a function at the highest level
  def getRecordIterator: Iterator[Array[ByteBuffer]] = {
    val it = getDataSource.toIterator
    val a = it.duplicate
    val buffer = ByteBuffer.allocate(a._2.length)
    for(c <- a._1) {
      buffer.put(c.toByte)
    }
    bufToSeq(buffer.rewind.asInstanceOf[ByteBuffer]).iterator
  }
  
  def bufToSeq(buffer: ByteBuffer) = {
    var s = Seq[Array[ByteBuffer]]()
    parseKey(buffer)
    parseVarint(buffer)
    val len = getOrigScalars.length
    while(buffer.hasRemaining){
      val arr = Array.ofDim[ByteBuffer](len)
      for(i <- 0 until len){
        arr(i) = firstVal(buffer)
      }
      s ++= Seq[Array[ByteBuffer]](arr)
    }
    s
  }
  
  def firstVal(buffer: ByteBuffer): ByteBuffer = {
    parseKey(buffer)._2 match {
      case 0 => {
        val arr = Array.ofDim[Byte](varintLength(buffer))
        buffer.get(arr)
        ByteBuffer.wrap(arr).rewind.asInstanceOf[ByteBuffer]
      }
      case 1 => ByteBuffer.allocate(8).putDouble(buffer.getDouble).rewind.asInstanceOf[ByteBuffer]
      case 2 => {
        val arr = Array.ofDim[Byte](parseVarint(buffer))
        buffer.get(arr)
        ByteBuffer.wrap(arr).rewind.asInstanceOf[ByteBuffer]
      }
      case 5 => ByteBuffer.allocate(4).putFloat(buffer.getFloat).rewind.asInstanceOf[ByteBuffer]
    }
  }
  
  def varintLength(buffer: ByteBuffer): Int = {
    buffer.mark
    var count = 1 
    while(buffer.hasRemaining && buffer.get<0) count += 1
    buffer.reset
    count
  }
  
  def parseRecord(record: Array[ByteBuffer]): Option[Map[String,Data]] = {
    val vars = getOrigScalars 
    val values = record.toSeq
    if (vars.length != values.length) None
    else {
      val vnames: Seq[String] = vars.map(_.getName)
      val datas: Seq[Data] = (values zip vars).map(p => parseBuffer(p._1, p._2))
      Some((vnames zip datas).toMap)
    }
  }
  
  def parseBuffer(value: ByteBuffer, variableTemplate: Variable): Data = variableTemplate match {
    case _: Integer => try {//TODO: check for sint
      Data(parseVarint(value).toLong)
    } catch {
      case e: NumberFormatException => Data(variableTemplate.asInstanceOf[Integer].getFillValue.asInstanceOf[Long])
    }
    case _: Real => try {
      Data(value.getDouble)
    } catch {
      case e: NumberFormatException => Data(variableTemplate.asInstanceOf[Real].getFillValue.asInstanceOf[Double])
    }
    case t: Text    => {//TODO: tuples
      val sb = new StringBuilder
      while(value.hasRemaining) sb append value.get.asInstanceOf[Char].toString
      Data(StringUtils.padOrTruncate(sb.toString, t.length))
    }
  }
  
  def parseVarint(buf: ByteBuffer): Int = {
    var count = 0 
    var num = 0
    val position = buf.position
    while(position + count < buf.capacity && buf.get(position + count)<0) {
      num += (buf.get & 0x7F)<<(7*count)
      count+=1
    }
    ((buf.get)<<(7*count)) + num
  }
  
  def parseKey(buf: ByteBuffer): (Int, Int) = {
    val n = parseVarint(buf)
    (n>>>3, n&0x7)
  }

}