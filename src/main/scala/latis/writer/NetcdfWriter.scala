package latis.writer

import java.io.OutputStream
import latis.dm.Dataset
import java.io.File

class NetcdfWriter extends FileWriter {

  override def writeFile(dataset: Dataset, file: File) = {
    val file = getFile
    
    //TODO
  }
  
}

object NetcdfWriter {

  def apply(fileName:String) = {
    val writer = new NetcdfWriter
    writer.setFile(new File(fileName))
    writer
  }
}