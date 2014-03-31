package latis.writer

import java.io.OutputStream
import latis.dm.Dataset
import java.io.File
import edu.ucar.ral.nujan.netcdf.NhFileWriter

class NetcdfWriter extends FileWriter {

  override def writeFile(dataset: Dataset, file: File) = {
    val ncfile = new NhFileWriter(file.getName, NhFileWriter.OPT_OVERWRITE)
    
    val rootGroup = ncfile.getRootGroup
    
    //assume dataset has one Function, for now
    val f = dataset.findFunction.get
    val domain = f.getDomain
    val range = f.getRange
    
    
    
  }
  
}

object NetcdfWriter {

  def apply(fileName:String) = {
    val writer = new NetcdfWriter
    writer.setFile(new File(fileName))
    writer
  }
}