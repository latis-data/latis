package latis.ops.filter

import latis.data.set.IndexSet
import latis.dm._
import latis.dm.Sample
import latis.dm.Tuple
import latis.ops.Operation
import latis.util.PeekIterator

/**
 * Subtype of Operation that may drop samples.
 */
class Filter extends Operation {
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
  
  override def applyToFunction(function: Function): Option[Function] = {
    val it = WrappedFunction(function, this).iterator
    it.isEmpty match {
      //return empty Function if empty
      //TODO: do we need to make sure domain and range are empty?
      case true => Some(Function(function.getDomain, function.getRange, function.getMetadata))  //empty data
      case false => Some(Function(function.getDomain, function.getRange, resetIndices(it), function.getMetadata))
    }
  }
  
  /**
   * Replaces the Domain in the Iterator of a Function with a new IndexSet.
   */
  def resetIndices(it: PeekIterator[Sample]): Iterator[Sample] = {
    if(it.peek.domain.isInstanceOf[Index]) IndexSet().iterator.zip(it).map(p => Sample(Index(p._1),p._2.range))
    else it
  }

}

