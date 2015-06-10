package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Sample
import latis.util.iterator.PeekIterator
import latis.dm.Scalar
import scala.collection.mutable.ArrayBuffer
import latis.dm.Tuple
import latis.dm.Function
import latis.ops.Reduction

class Intersection extends Aggregation {
  //use case: x -> (a,b) intersect x -> (c,d) => x -> (a,b,c,d) 
  //  where only samples with x in both are kept

  //assumes dataset is the parent of a tuple that has the variables of the datasets to aggregate
  def aggregate(dataset: Dataset): Dataset = {    
    val fs = getFunctions(dataset)
    //assume just 2 functions for now //TODO: fold
    val it1 = fs(0).iterator
    val it2 = fs(1).iterator
    
    val reduction = new Reduction
    
    //need domain and range types for new Function
    val dtype = fs(0).getDomain
    val rtype = reduction.applyToTuple(Tuple(fs(0).getRange, fs(1).getRange)).get //flatten, consistent with below
    
    //make sample iterator
    val samples = new PeekIterator[Sample]() {
      def getNext = {
        getNextMatchingSamplePair(it1, it2) match {
          case Some((s1, s2)) => {
            //use common domain sample
            val domain = s1.domain
            //merge range values, reduce so we don't end up with extra Tuple nesting
            //TODO: is reduce always appropriate? maybe just deal with this one layer we added? did this here because if iterable once problem?
            val range = reduction.applyToTuple(Tuple(s1.range, s2.range)).get //Option
            Sample(domain, range)
          }
          case None => null
        }
      }
    }
    
    //keep original metadata
    val md = dataset.getMetadata
    Dataset(Function(dtype, rtype, samples), md)
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
  
  def apply() = new Intersection()
}