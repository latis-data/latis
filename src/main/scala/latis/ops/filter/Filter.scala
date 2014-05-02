package latis.ops.filter

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.ops.Operation

/**
 * Subtype of Operation that may drop samples.
 */
class Filter extends Operation {
  //TODO: see scala's withFilter, FilterMonadic
  
  /**
   * Default filter for Samples. Handle like any other Tuple: exclude if any element is invalid.
   */
  override def applyToSample(sample: Sample): Option[Sample] = sample.getVariables.map(applyToVariable(_)).find(_.isEmpty) match {
    case Some(_) => None //found an invalid variable, exclude the entire tuple
    case None => Some(sample)
  }
  
  /**
   * Default filter for Tuples. Filter each element and exclude the entire Tuple if any element is invalid (None).
   */
  override def applyToTuple(tuple: Tuple): Option[Tuple] = tuple.getVariables.map(applyToVariable(_)).find(_.isEmpty) match {
    case Some(_) => None //found an invalid variable, exclude the entire tuple
    case None => Some(tuple)
  }

}

