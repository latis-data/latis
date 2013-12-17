package latis.writer

import java.io.OutputStream
import java.io.DataOutputStream
import latis.dm._

class BinaryWriter extends Writer {

  private[this] lazy val writer = new DataOutputStream(outputStream)
  
  def write(dataset: Dataset) {
    dataset.getVariables.map(writeVariable(_))
    writer.flush()
  }
  
  def writeVariable(variable: Variable) {
    val data = variable.getData
    if (data.isEmpty) variable match {
      case Tuple(vars) => vars.map(writeVariable(_))
      case f: Function => f.iterator.map(writeVariable(_))
      case _ => throw new Error("No Data found.")
    } else {
      val bs = data.getByteBuffer.array
      writer.write(bs)
    }
  }
  
}

object BinaryWriter {
  
  def apply(out: OutputStream): BinaryWriter = {
    val writer = new BinaryWriter()
    writer._out = out
    writer
  }
  
  def apply(): BinaryWriter = BinaryWriter(System.out)
}