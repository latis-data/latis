package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.DataOutputStream
import java.io.OutputStream
import java.io.PrintWriter

class DataDdsWriter extends BinaryWriter {
  
  private[this] lazy val writer = new DataOutputStream(outputStream)

  final val START_OF_INSTANCE: Array[Byte] = Array(0xA5.toByte, 0, 0, 0)
  
  final val END_OF_INSTANCE: Array[Byte] = Array(0x5A.toByte, 0, 0, 0)
  
  override def write(dataset: Dataset) {
    writeHeader(dataset)
    dataset.getVariables.map(writeVariable(_))
    writer.flush()
  }
  
  def writeHeader(dataset: Dataset) = {
    //val w = new DdsWriter()
    //writer.write((w.makeHeader(dataset)+w.varToString(dataset)+w.makeFooter(dataset)).toByte)
    Writer("dds").write(dataset)
    println("Data:")
  }
  
  override def writeVariable(variable: Variable) {
    val data = variable.getData
    if (data.isEmpty) variable match {
      case Tuple(vars) => vars.map(writeVariable(_))
      case f: Function => {
        for(a <- f.iterator) {
          writer.write(START_OF_INSTANCE)
          writeVariable(a)
          }
        writer.write(END_OF_INSTANCE)
      }
      case _ => throw new Error("No Data found.")
    } 
    else {
      val bs = data.getByteBuffer.array
      writer.write(bs)
    }
  }
}