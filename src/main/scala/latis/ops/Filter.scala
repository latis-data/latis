package latis.ops

import latis.dm.Dataset
import latis.dm.FilteredFunction
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable

import scala.Option.option2Iterable

/**
 * Subtype of Operation that may drop samples.
 */
trait Filter extends Operation {
  
  /**
   * Apply this Operation to the given Dataset.
   */
  def apply(dataset: Dataset): Dataset = filter(dataset)
    
  /**
   * Apply this Filter to the given Dataset.
   * Same as 'apply' but more semantically useful.
   */
  def filter(dataset: Dataset): Dataset = {
    Dataset(dataset.getVariables.flatMap(filterVariable(_)))
    //TODO: provenance metadata
  }
  
  protected def filterVariable(variable: Variable): Option[Variable] = variable match {
    case scalar: Scalar     => filterScalar(scalar)
    case sample: Sample     => filterSample(sample)
    case tuple: Tuple       => filterTuple(tuple)
    case function: Function => filterFunction(function)
  }
  
  /**
   * No-op default filter for Scalars.
   */
  protected def filterScalar(scalar: Scalar): Option[Scalar] = Some(scalar)
  
  /**
   * Default filter for Samples. Handle like any other Tuple: exclude if any element is invalid.
   */
  def filterSample(sample: Sample): Option[Sample] = sample.getVariables.map(filterVariable(_)).find(_.isEmpty) match {
    case Some(_) => None //found an invalid variable, exclude the entire tuple
    case None => Some(sample)
  }
  
  /**
   * Default filter for Tuples. Filter each element and exclude the entire Tuple if any element is invalid (None).
   */
  protected def filterTuple(tuple: Tuple): Option[Tuple] = tuple.getVariables.map(filterVariable(_)).find(_.isEmpty) match {
    case Some(_) => None //found an invalid variable, exclude the entire tuple
    case None => Some(tuple)
  }
  
  /**
   * Wrap Function with a FilteredFunction.
   */
  protected def filterFunction(function: Function): Option[Function] = Some(FilteredFunction(function, this))

}

