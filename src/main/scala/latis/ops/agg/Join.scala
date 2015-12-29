package latis.ops.agg

import latis.dm.Dataset
import latis.ops.BinaryOperation
import latis.dm.Sample
import latis.dm.Tuple
import latis.dm.Scalar
import latis.dm.Variable

/**
 * Base type for Operations that join (combine) Datasets.
 */
trait Join extends BinaryOperation {
  //Designed to replace Aggregation. See LATIS-325
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset
      
  def joinVariables(v1: Variable, v2: Variable): Option[Variable] = (v1,v2) match {
    //TODO: preserve named tuple namespace
    //TODO: deal with duplicate var names
    case (Tuple(vars1), Tuple(vars2)) => Some(Tuple(vars1 ++ vars2))
    case (Tuple(vars1), s2: Scalar) => Some(Tuple(vars1 :+ s2))
    case (s1: Scalar, Tuple(vars2)) => Some(Tuple(s1 +: vars2))
    case (s1: Scalar, s2: Scalar) => Some(Tuple(Seq(s1, s2)))
    //TODO: nested Functions
  }
  
  def joinSamples(sample1: Sample, sample2: Sample): Option[Sample] = {
    //TODO: make sure name and units match
    //Assumes scalar domain, for now
    val domain1 = sample1.domain.asInstanceOf[Scalar]
    val domain2 = sample2.domain.asInstanceOf[Scalar]
    if (domain1.compare(domain2) != 0 ) None
      //val msg = "Samples must have matching domain variables to be joined."
      //TODO: log? throw new UnsupportedOperationException(msg)
    else joinVariables(sample1.range, sample2.range) match {
      case Some(v) => Some(Sample(domain1, v))
      case None => None
    }
  }
  
}
