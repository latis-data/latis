package latis.ops.filter

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.ops.Operation
import latis.dm.WrappedFunction
import latis.ops.Idempotence

/**
 * Subtype of Operation that may drop samples.
 */
class Filter extends Operation with Idempotence {
  //TODO: see scala's withFilter, FilterMonadic
  
  /*
   * Nested Function
   * if inner domain value is filtered out, just drop the internal sample
   * if range value is filtered out, drop entire outer sample?
   *   need to maintain same samples if cartesian
   * 
   * do we need to process entire inner Function instead of trying to wrap it?
   * 
   * 
   */
  
  /**
   * Default filter for Samples. Handle like any other Tuple: exclude if any element is invalid.
   */
  override def applyToSample(sample: Sample): Option[Sample] = sample.getVariables.map(applyToVariable(_)).find(_.isEmpty) match {
    case Some(_) => None //found an invalid variable, exclude the entire tuple
    case None => Some(sample)
  }
  
  /**
   * Do not apply filter to Tuples.
   */
  override def applyToTuple(tuple: Tuple): Option[Tuple] = {
    Some(tuple)
  }
  
  /**
   * Override so we get an empty Function if no samples match instead of None.
   */
  override def applyToFunction(function: Function): Option[Function] = {
    val it = WrappedFunction(function, this).iterator
    it.isEmpty match {
      //return empty Function if empty
      //TODO: do we need to make sure domain and range are empty?
      case true  => Some(Function(function.getDomain, function.getRange, function.getMetadata))  //empty data
      case false => Some(Function(function.getDomain, function.getRange, it, function.getMetadata))
    }
  }


}

