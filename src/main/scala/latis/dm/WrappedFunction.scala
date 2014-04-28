package latis.dm

import latis.ops.SampleMappingOperation
import latis.util.IndexedIterator
import latis.util.PeekIterator2
import latis.ops.Projection
import latis.data.SampleData
import latis.util.DataUtils

//TODO: SampleMappedFunction?
class WrappedFunction(function: Function, val operation: SampleMappingOperation) 
  extends SampledFunction(function.getDomain, function.getRange) {
  
  /**
   * Override iterator to apply the Operation to each sample as it iterates (lazy).
   */
  override def iterator: Iterator[Sample] = new PeekIterator2(function.iterator, (s: Sample) => operation(s))
  
  //only needed for projection?
  override def getDataIterator: Iterator[SampleData] = iterator.map(DataUtils.sampleToData(_))

}

object WrappedFunction {
  def apply(function: Function, operation: SampleMappingOperation) = new WrappedFunction(function, operation)
}
  
  /*
   * +++ Use Case: Operation (e.g. Projection) replaces domain with Index
   * orig SampledFunction has SampledData from single iterator
   *   if SF had been defined with Index, SD would have IndexSet and rangeData
   * do we need to munge SampledData to have IndexSet?
   * the new domain/range types should be right
   * 
   * looks like trying to apply to sample for each sample instead of applying to range
   * do we need to make a new SampledFunction with new SampledData with IndexSet?
   * needs to be part of op.applyToFunction?
   * is Projection the only op that has this issue?
   * maybe it shouldn't try to reuse the standard WrappedFunction
   * maybe even apply projection at dataMap level?
   *   adapter.makeFunction maps dataMap to Data, op can't touch that
   * proj could get SD from SF, could SD encapsulate mapping f for map to data?
   *   just need to feed it new sampleTemplate?
   *   dig into the SD iterator (Peek2)? unlikely
   *   
   * if SD has domainSet then no problem, just replace it with Index
   * can always realize domain set from iterative SD, but requires iterating all?
   * 
   * not just a problem for index domain
   * any non projected data needs to be filtered out of the Data
   * how did we do this before?
   *   had complete Sample filled then operated on that
   *   isn't that what we are doing now?
   *   applyToSample
   *   only if orig SampledData has IndexSet does the adapter do anything special
   * 
   * Projection also has unique variable order issue
   * Maybe Projection calls for something unique
   * how could it do var order?
   *   sampleTemplate would have new order
   *   problem if domain is not first?
   *   
   * did I say somewhere that FooOp would only call applyToSample once to get types then just apply to range if domain was IndexSet?
   * see Projection applyToSample
   *   how will wrapped function enforce this?
   *   need to be getting index value via iteration on IndexSet
   *   unless we go back to managing index
   * Filter has index issue, too
   * both seem to come down to needing to replace SampledData or insert index values
   * 
   * Replace SampledData in WrappedFunction
   * we do have orig sampleTemplate so we can deconstruct orig Data
   * only a problem if we don't have a domain set?
   * ++Always make with domain set in adapter?!!!
   *   that could at least be a good start
   *   still need to alter data for non-proj range vars, but can do that at the sample level
   *   but each needs to feed off of same record iterator
   * 
   * Where should SampledData replacement happen?
   * just do in ProjectedFunction so we don't clutter up this?
   * when applying to get new domain, range
   * if domain is Index
   * note, this iterator wraps f.iterator which often? comes from 
   *   getDataIterator.map(DataUtils.dataToSample(_, Sample(domain, range)))
   *   DSL may make SampledFunction with Iterator[Sample]
   *   getDataIterator is just SampledData.iterator
   *   just make sure SampledData matches sample used in dataToSample
   *   proj may want to drop other vars, but may be easier to do with samples instead of data
   * 
   * * See how Projection applyToFunction deals with type changes before delegating to WrappedFunction
   * but still need to munge type since we didn't munge data
   * do we need ProjectedFunction for that, or just more work in Projection?
   * 
   * ++require Op to munge model and data so we don't have to worry about changing domain and range here
   * 
   * bug projecting WrappedFunction (with selection)
   * getData = null
   * do we need to use getDataIterator? 
   *   sounds like a mess
   * always do projection first?
   * 
   * +++
   */
  
  
  
  /*
   * TODO: if domain ends up as Index, replace DomainSet with IndexSet
   * IterativeAdapter provides Data Iterator
   *   ++could it provide SampledData with a DomainSet?
   * SampledFunction uses DataUtils.dataToSample in its iterate
   * too late to change Data Iterator!?
   * 
   * +++Could wrapped Functions act on the Data instead of wrapping the iterator?
   * we have it all here, it's only 'iterator' that calls function.iterator
   * more opportunities to munge data in diff ways
   * not limited to one sample at a Time!
   * would require Function to have Data, consistent with recent refactorings
   * 
   * what does that mean for Operations to work on samples?
   *   probably still would
   *   wrapped function could first apply op to model
   *     if we end up with Index (projection) replace domain set with index set
   * but how can we take Adapter's Data Iterator and replace it with a DomainSet?
   *   should adapter provide SampledData?
   *   but IterativeAdapter does only supply one sample at a time
   *   maybe it could provide SampleData (without 'd'): pair of Data, domain and range separate
   *     basic DomainSet could simply wrap Iterator of domain Data
   *   or adapter could make it as a SampledData and do the wrapping itself
   * 
   * should Adapters have the cache or should IterableData?
   * was impld with Stream
   * less going back and forth between Variables and Data
   * just need to make sure that we can call iterator on a Function and invoke a new Data iterator
   * 
   * problem having separate iterators within SampledData?
   * must keep in sync
   * at least domain set should cache so it can iterate again?
   * 
   */
