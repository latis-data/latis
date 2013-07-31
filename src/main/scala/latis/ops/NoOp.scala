package latis.ops

import latis.dm.Dataset

class NoOp extends Operation {

  def apply(dataset: Dataset): Dataset = dataset

}