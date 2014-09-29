package latis.reader.tsml

import java.io.DataInputStream
import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

import latis.data.Data
import latis.data.seq.DataSeq
import latis.dm.Integer
import latis.dm.Real
import latis.reader.tsml.ml.Tsml

class SsiAdapter(tsml: Tsml) extends TsmlAdapter(tsml){
  
  override def init {
    for (v <- getOrigScalars) {
      val vname = v.getName
      val location = getUrl.getPath + vname + ".bin"
      val file = new File(location)
      val order = this.getProperty("byteOrder", "big-endian") match {
        case "big-endian" => ByteOrder.BIG_ENDIAN
        case "little-endian" => ByteOrder.LITTLE_ENDIAN
      }
      
      val is = new DataInputStream(new FileInputStream(file))
      val arr = Array.ofDim[Byte](file.length.toInt)
      is.readFully(arr)
      val bb = ByteBuffer.wrap(arr)
      bb.order(order)
      
      val n = bb.limit/v.getSize
      val datas = v match {
        case i: Integer => (0 until n).map(i => bb.getLong()).map(Data(_))
        case r: Real    => (0 until n).map(i => bb.getDouble()).map(Data(_))
//        case t: Text    => (0 until n).map(bb.getObject(_)).map(o => Data(o.toString))
      }
      
      val data = DataSeq(datas)
      
      cache(vname, data)
      
      is.close
    }
  }
  
  def close = {}

}