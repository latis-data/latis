package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Sample
import latis.util.iterator.PeekIterator
import latis.dm.Scalar
import scala.collection.mutable.ArrayBuffer
import latis.dm.Tuple
import latis.dm.Function
import latis.ops.Reduction
import latis.metadata.EmptyMetadata
import latis.metadata.Metadata
import latis.ops.ReduceTuple
import latis.dm.Variable

class Intersection extends Aggregation { 
  //use case: x -> (a,b) intersect x -> (c,d) => x -> (a,b,c,d) 
  //  where only samples with x in both are kept

  def aggregate(ds1: Dataset, ds2: Dataset): Dataset = {    
    val (f1, f2, it1, it2) = (ds1, ds2) match {
      case(Dataset(f1 @ Function(it1)), Dataset(f2 @ Function(it2))) => (f1, f2, it1, it2)
      case _ => throw new UnsupportedOperationException("Intersection expects a Function in each of the Datasets it aggregates.")
    }
    
    //need domain and range types for new Function
    val dtype = f1.getDomain
    val rtype = joinRanges(f1.getRange, f2.getRange) //flatten, consistent with below
    
    //make sample iterator
    val samples = new PeekIterator[Sample]() {
      def getNext = {
        getNextMatchingSamplePair(it1, it2) match {
          case Some((s1, s2)) => {
            //use common domain sample
            val domain = s1.domain
            //merge range values, reduce so we don't end up with extra Tuple nesting
            val range = joinRanges(s1.range, s2.range)
            Sample(domain, range)
          }
          case None => null
        }
      }
    }
    
    //TODO: update metadata
    //use metadata from first Dataset for new Dataset
    //Note, dataset metadata must be passed in via the super apply(ds1, ds2, md)
    val fmd = f1.getMetadata
    
    Dataset(Function(dtype, rtype, samples, fmd))
  }
  
  /**
   * Combine Scalars and Tuples into a single Tuple.
   * Does not support Functions in the range for now.
   */
  private def joinRanges(v1: Variable, v2: Variable): Variable = (v1,v2) match {
    case (Tuple(vars1), Tuple(vars2)) => Tuple(vars1 ++ vars2)
    case (Tuple(vars1), s2: Scalar) => Tuple(vars1 :+ s2)
    case (s1: Scalar, Tuple(vars2)) => Tuple(s1 +: vars2)
    case (s1: Scalar, s2: Scalar) => Tuple(Seq(s1, s2))
  }
  
  private def getNextMatchingSamplePair(it1: Iterator[Sample], it2: Iterator[Sample]): Option[(Sample,Sample)] = {
    if (! it1.hasNext || ! it2.hasNext) None
    else {
      //recursive helper method
      def findMatchingSample(s1: Sample, s2: Sample): Option[(Sample,Sample)] = {
        val d1 = s1.domain.asInstanceOf[Scalar]
        val d2 = s2.domain.asInstanceOf[Scalar]
        val comparison = d1 compare d2
        
        if (comparison == 0) Some((s1, s2))
        else if (comparison > 0 && it2.hasNext) findMatchingSample(s1, it2.next)
        else if (comparison < 0 && it1.hasNext) findMatchingSample(it1.next, s2)
        else None //one of the Iterators ran out
      }
      
      val sample1 = it1.next
      val sample2 = it2.next
      findMatchingSample(sample1, sample2)
    }
  }
}

object Intersection {
  
  def apply(): Intersection = new Intersection()
  def apply(ds1: Dataset, ds2: Dataset, md: Metadata = EmptyMetadata): Dataset = new Intersection()(ds1, ds2, md)
}