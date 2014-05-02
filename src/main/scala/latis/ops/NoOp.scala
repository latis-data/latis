package latis.ops

import latis.dm.Dataset

/**
 * Implementation of Operation that has no effect.
 */
class NoOp extends Operation {

  override def apply(dataset: Dataset): Dataset = dataset

}