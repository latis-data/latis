package latis.ops.xform

import latis.dm._
import latis.util.LatisProperties
import latis.ops.Operation
import latis.metadata.Metadata

/**
 * Subtype of Operation that may modify a Dataset beyond just dropping samples (Filter).
 * The data types may not be preserved.
 */
@Deprecated
trait Transformation extends Operation {
  
//  /**
//   * Apply this Operation to the given Dataset.
//   */
//  override def apply(dataset: Dataset): Dataset = transform(dataset)
//    
//  /**
//   * Apply this Transformation to the given Dataset.
//   * Same as 'apply' but more semantically useful.
//   */
//  def transform(dataset: Dataset): Dataset = {
//    Dataset(dataset.getVariables.map(transformVariable(_)))
//    //TODO: provenance metadata
//  }
//  
//  protected def transformVariable(variable: Variable): Variable = variable match {
//    case scalar: Scalar     => transformScalar(scalar)
//    case sample: Sample     => transformSample(sample)
//    case tuple: Tuple       => transformTuple(tuple)
//    case function: Function => transformFunction(function)
//  }
//  
//  /**
//   * No-op default transform for Scalars.
//   */
//  protected def transformScalar(scalar: Scalar): Variable = scalar
//  
//  /**
//   * Default transform for Samples. Transform the range component leaving the domain unchanged.
//   */
//  def transformSample(sample: Sample): Variable = Sample(sample.domain, transformVariable(sample.range))
//  
//  /**
//   * Default transform for Tuples. Transform each member then repackage as a Tuple.
//   */
//  protected def transformTuple(tuple: Tuple): Variable = Tuple(tuple.getVariables.map(transformVariable(_)))
//  
//  /**
//   * Wrap Function with a FilteredFunction.
//   */
//  protected def transformFunction(function: Function): Variable = TransformedFunction(function, this)
 
}
