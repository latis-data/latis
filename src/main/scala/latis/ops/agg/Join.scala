package latis.ops.agg

import latis.dm._
import latis.ops._

/**
 * Base type for Operations that join (combine) Datasets.
 */
class Join extends BinaryOperation {
  //Designed to replace Aggregation. See LATIS-325
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    (ds1, ds2) match {
      case (Dataset(v1), Dataset(v2)) => (v1, v2) match {
        case (f1 @ Function(it1), f2 @ Function(it2)) => {
          val samples = (it1 zip it2).flatMap(p => joinSamples(p._1, p._2))
          Dataset(Function(f1.getDomain, f1.getRange, samples, f1.getMetadata))
        }
        case _ => throw new UnsupportedOperationException("Join expects a Function in each of the Datasets it joins.")
      }
      case (Dataset(_), _) => ds1 //ds2 empty
      case (_, Dataset(_)) => ds2 //ds2 empty
      case _ => Dataset.empty //both datasets empty
    }
  }
      
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
      //TODO: be clever about skipping gap in one dataset
      //val msg = "Samples must have matching domain variables to be joined."
      //TODO: log? throw new UnsupportedOperationException(msg)
    else joinVariables(sample1.range, sample2.range) match {
      case Some(v) => Some(Sample(domain1, v))
      case None => None
    }
  }
  
}

object Join {
  def apply(): Join = new Join
  def apply(ds1: Dataset, ds2: Dataset): Dataset = new Join()(ds1, ds2)
}
