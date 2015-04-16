package latis.ops

import latis.dm.Dataset

class Sleep(milliseconds: Int) extends Operation {
  
  override def apply(ds: Dataset): Dataset = {
    Thread.sleep(milliseconds)
    ds
  }

}

object Sleep extends OperationFactory {
  
  override def apply(args: Seq[String]): Operation = new Sleep(args(0).toInt)
  
  def apply(arg: String): Operation = new Sleep(arg.toInt)
  
  def apply(milliseconds: Int): Operation = new Sleep(milliseconds)
  
}