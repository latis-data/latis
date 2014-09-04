package latis.reader.tsml

import java.io.DataInputStream
import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import scala.collection.mutable.ListBuffer
import latis.data.Data
import latis.data.seq.DataSeq
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text
import latis.reader.tsml.ml.Tsml
import java.nio.ByteOrder

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
      //Some names contain "." which findVariable will interpret as a structure member
      //NetCDF library dropped NetcdfFile.escapeName between 4.2 and 4.3 so replicate with what it used to do.
      //TODO: replace with "_"?
      //val escapedName = EscapeStrings.backslashEscape(vname, ".") 
      //val vname = vname.replaceAll("""\.""", """\\.""")
      val is = new DataInputStream(new FileInputStream(file))
      val arr = Array.ofDim[Byte](file.length.toInt)
      is.readFully(arr)
      val bb = ByteBuffer.wrap(arr)
      bb.order(order)
      
      val n = bb.limit/v.getSize
      
      //val ncvar = ncFile.findVariable(escapedName)
      
      //Get data Array from Variable
      //val ncarray = ncvar.read
      //TODO: apply section
      
      //val n = ncarray.getSize.toInt //TODO: limiting length to int
      //val ds = (0 until n).map(ncarray.getObject(_)).map(Data(_)) //Let Data figure out how to store it, assuming primitive type

//      val datas = new ListBuffer[Data]()
//      
//      while(bb.hasRemaining) v match {
//        case i: Integer => datas += Data(bb.getLong)
//        case r: Real => datas += Data(bb.getDouble)
//        case t: Text => ???
//      }
      
      //Store data based on the type of varible as defined in the tsml
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