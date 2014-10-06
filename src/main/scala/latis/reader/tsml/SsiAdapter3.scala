package latis.reader.tsml

import java.io.DataInputStream
import latis.dm.Function
import scala.collection.mutable
import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import latis.data.Data
import latis.data.seq.DataSeq
import latis.dm.Integer
import latis.dm.Real
import latis.reader.tsml.ml.Tsml
import java.io.RandomAccessFile
import java.nio.channels.FileChannel.MapMode
import latis.dm.Variable
import latis.util.DataUtils
import latis.dm.Sample
import latis.util.BufferIterator
import latis.dm.Scalar
import latis.data.IterableData
import latis.data.SampledData
import latis.dm.Tuple
import latis.data.EmptyData

class SsiAdapter3(tsml: Tsml) extends TsmlAdapter(tsml){
  
  override def init {
    for (v <- getOrigScalars) {
      val vname = v.getName
      val location = getUrl.getPath + vname + ".bin"
      val file = new RandomAccessFile(location, "r")
      val order = this.getProperty("byteOrder", "big-endian") match {
        case "big-endian" => ByteOrder.BIG_ENDIAN
        case "little-endian" => ByteOrder.LITTLE_ENDIAN
      }
      val bb = file.getChannel.map(MapMode.READ_ONLY, 0, file.length)
      bb.order(order)
      
      val datas = DataSeq(new BufferIterator(bb, v))
      
      cache(vname, datas)
      
    }
  }
  
  def close = {}

}