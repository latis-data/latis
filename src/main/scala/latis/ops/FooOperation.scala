package latis.ops

import latis.dm.Dataset
import latis.dm.Real

class FooOperation(args: Seq[String]) extends Operation {

  def apply(dataset: Dataset): Dataset = {
    println("applying FooOperation")
    dataset / Dataset(Real(2.0))
    
  }
}