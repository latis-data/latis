package latis.ops.xform

import latis.dm._
import latis.util.LatisProperties
import latis.ops.Operation
import latis.metadata.Metadata

abstract class Transformation extends Operation {

  def apply(dataset: Dataset): Dataset = Dataset(dataset.getVariables.map(transform(_)), dataset.getMetadata)
  //TODO: provenance metadata...
  
  def transform(variable: Variable): Variable = variable match {
    case s: Scalar => transformScalar(s)
    case sample: Sample => transformSample(sample)
    case t: Tuple => transformTuple(t)
    case f: Function => transformFunction(f)
  }
  
  //type not necessarily preserved
  def transformScalar(scalar: Scalar): Variable = scalar //no-op
  def transformTuple(tuple: Tuple): Variable = Tuple(tuple.getVariables.map(transform(_)))
  def transformFunction(function: Function): Variable = TransformedFunction(function, this)
    //TODO: or Function(transform(function.domain), transform(function.range)) ?
    
  //typically invoked by TransformedFunction
  //sample should be case where we just act on range, else use tuple
  def transformSample(sample: Sample): Sample = Sample(sample.domain, transform(sample.range))
  

 
}





