package latis.ops.agg

import latis.dm.Dataset
import latis.dm.Sample
import latis.util.PeekIterator
import latis.dm.Scalar

class Intersection extends Aggregation {
  //use case: x -> (a,b) intersect x -> (c,d) => x -> (a,b,c,d) 
  //  where only samples with x in both are kept

  //assumes dataset is the parent of the datasets to aggregate
  def aggregate(dataset: Dataset): Dataset = {
    //child datasets to aggregate
    val datasets = dataset.getVariables.map(_.asInstanceOf[Dataset])
    
    //assume just 2 for now
    //get functions from each, assuming each ds has only one
    val fs = datasets.flatMap(_.findFunction)
    
    //make MappingIterator that only keeps samples that exist in both
    //val mappingFunction = 
      
    //TODO: act on Samples or Data?
    /*
     * union domain sets, iterate over that keeping samples that have that value
     * 'evaluate' functions?
     * make Map of samples domain Data -> range Data
     * join range data: simply append?
     * use Data as hash key? bytes
     * but need solution that works with long datasets
     *   take advantage of them being sorted
     *   but can't compare with Data alone, need type info
     *   use Scalar compare? 
     *   will that extend to tuple domains? or do we need to use DomainSet?
     * can we make SampledData without having to separate domain and range? we have constructor for it
     * consider how this could be used for other aggregators with a simple switch of logic
     *   intersect: drop if not in both
     *   union: add fill data
     * 
     * or use function iterators:
     *   takes advantage of sorting, compare
     * 
     * getNextMatchingSample(fit1, fit2)
     * equivalent vs equal? e.g. same metadata, unit conversion
     * 
     */
    
    ???
  }
  
  private def getNextMatchingSample(it1: Iterator[Sample], it2: Iterator[Sample]): Option[Sample] = {
    if (! it1.hasNext || ! it2.hasNext) None
    else {
      val sample1 = it1.next
      val sample2 = it2.next
      
      //assume Scalar domains for now, since only it impls compare, but only with String value
      val d1 = sample1.domain.asInstanceOf[Scalar]
      val d2 = sample2.domain.asInstanceOf[Scalar]
    
//      d1.compare(d2) match {
//        
//      }
      ???
    }
  }

}