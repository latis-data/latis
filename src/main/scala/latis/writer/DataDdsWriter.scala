package latis.writer

import latis.dm._
import java.nio.ByteBuffer
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
    val w = new DdsWriter()
    val s = w.makeHeader(dataset) + w.varToString(dataset) + w.makeFooter(dataset) + "Data:"
    writer.write(s.getBytes)    
  }
  
  override def writeVariable(variable: Variable) = variable match {
    case f: Function => {
      for (sample <- f.iterator){
        writer.write("\n".getBytes)
        writer.write(START_OF_INSTANCE)
        writer.write(varToBytes(sample))
      }
      writer.write(END_OF_INSTANCE)
    }
    case _ => {
      writer.write("\n".getBytes)
      writer.write(varToBytes(variable))
    }
  }
 
  override def buildFunction(function: Function, bb: ByteBuffer): ByteBuffer = {
     writeVariable(function)
     bb
  }

}