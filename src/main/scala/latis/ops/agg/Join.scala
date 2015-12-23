package latis.ops.agg

import latis.dm.Dataset
import latis.ops.BinaryOperation

/**
 * Base type for Operations that join (combine) Datasets.
 */
trait Join extends BinaryOperation {
  //Designed to replace Aggregation. See LATIS-325
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset
      
}
