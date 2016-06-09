package latis.ops

import scala.Option.option2Iterable
import scala.reflect.runtime.currentMirror

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.util.LatisProperties
import latis.util.iterator.MappingIterator
import latis.dm.Naught

/**
 * Experimental alternative to Operation.
 * - maps over iterator instead of using MappingIterator
 * - applies to both domain and codomain of Functions 
 * - uses Naught for domain and codomain types to see if they are really needed
 * TODO: CoDomainOperation, e.g. math, like Spark mapValues
 *   can benefit knowing that domain won't change
 */
abstract class Operation2 {
  
  /**
   * Apply this Operation to the given Dataset.
   */
  def apply(dataset: Dataset): Dataset = dataset match {
    case Dataset(variable) => {
      val md = dataset.getMetadata
      //TODO: delegate to subclass to munge metadata
      //TODO: add provenance metadata, getProvMsg, append to "history"
      applyToVariable(variable) match {
        case Some(v) => Dataset(v, md)
        case None => Dataset.empty //TODO: preserve type metadata?
      }
    }
    case _ => dataset //dataset is empty
  }
  
  /**
   * Apply Operation to a Variable.
   */
  def applyToVariable(variable: Variable): Option[Variable] = variable match {
    case scalar: Scalar     => applyToScalar(scalar)
    case sample: Sample     => applyToSample(sample)
    case tuple: Tuple       => applyToTuple(tuple)
    case function: Function => applyToFunction(function)
  }

  /**
   * Default no-op operation for Scalars.
   */
  def applyToScalar(scalar: Scalar): Option[Variable] = Some(scalar)
  
  /**
   * Default operation for Samples. Apply operation domain and codomain.
   */
  def applyToSample(sample: Sample): Option[Sample] = sample match {
    case Sample(domain, codomain) => {
      for (d <- applyToVariable(domain); c <- applyToVariable(codomain))
        yield Sample(d,c)
    }
  }
  
  /**
   * Default operation for Tuples. Apply operation to each element.
   * If all elements are invalid, then the Tuple is invalid.
   */
  def applyToTuple(tuple: Tuple): Option[Variable] = {
    //TODO: should we allow an empty Tuple? probably not
    //  should we reduce a Tuple of one to the thing? consider that it is a Product type
    val vars = tuple.getVariables.flatMap(applyToVariable(_))
    if (vars.length == 0) None
    else Some(Tuple(vars, tuple.getMetadata))
  }
  
  /**
   * Default operation for a Function. Map operation over each sample.
   */
  def applyToFunction(function: Function): Option[Variable] = {
    //TODO: preserve memoization
    /*
     * TODO: avoid having to read data to get Function type
     * construct Function with function that provides domain and range?
     * Do we really need the types? see how far we get with Naught
     */
    val samples = function.iterator.flatMap(applyToSample(_))
    Some(Function(Naught(), Naught(), samples, function.getMetadata))
  }
  
}
