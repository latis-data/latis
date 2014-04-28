package latis.data

import latis.data.set.DomainSet
import latis.dm.Sample

class SampledData extends IterableData {
  //TODO: manage caching here? special subclass?
  
  def recordSize: Int = domainSet.recordSize + rangeData.recordSize
  
  private var _domain: DomainSet = null
  private var _range: IterableData = null
  
  //TODO: if null, iterate and cache both?
  def domainSet: DomainSet = _domain
  def rangeData: IterableData = _range
    //TODO: if _domain == null, need to realize (and cache) it
    //problem if _iterator is IterableOnce, need to cache both
    
  
  /*
   * problem managing domain and range data separately 
   * since they are often defined in terms of iterators that need to be synced
   * impl as getters, cache each IterableOnce (to Stream?) upon first iteration
   * but how will this affect long data?
   *   only getDomain, get Range would do cache, direct iterator wouldn't
   * what if we want it to cache (as opposed to Adapter managing cache)?
   * special subclass that the adapter could choose to use?
   * 
   * Projection may leave domain as Index
   * need IndexSet
   * would have to restructure
   * ++or wrap
   *   but wouldn't know what data to drop
   * would be nice to avoid making Data out of all non projected vars
   * 
   * Need to be able to make SampledData with domain and range both tied to the same iterator
   * such that we can make new SampledData replacing the domain set (e.g with IndexSet)
   * also need to be careful about someone iterating on the orig.
   * may be better to just plan on caching here?
   * let's try to avoid that
   * only if domainSet and rangeData are not accessed individually
   * 
   * Use case: domain set replaced with index set
   * no need to iterate, rangeData can now own orig
   * so if rangeData is requested, need to realize and cache domainSet
   *   if not already indep of iterator
   *   _domain == null implies that it does depend on _iterator
   *   so only need to do it _domain == null
   *   problem if IterableOnce
   * another mechanism for replacing domain set without triggering iter/cache
   *   replaceDomain(dset)
   *   still requires orig SD made with domain set, but it can be coupled to range
   * 
   * +see Iterator.duplicate!
   *   may cache as needed, but just what we need
   *   waste if we no longer want orig dset,
   *   but orig SD would remain valid
   */
  /**
   * Return a new SampledData with the given domainSet but the original
   * range Data without invoking its iterator.
   */
//  def replaceDomain(newDomainSet: DomainSet): SampledData = {
//    SampledData(newDomainSet, _range)
//    /*
//     * is it enough to do: SampledData(newDomainSet, _range)
//     * where does that leave the original?
//     * the orig SampledFunction would still have it
//     * might be able to get away with it in server but dangerous in DSL
//     * dare we make Data mutable? hope not
//     * hazards of iterable once that we might have to suffer for long data?
//     * to be safe, this new SD should have cached data
//     * try Iterator.duplicate
//     * 
//     * +could our PeekIterator optionally cache?
//     * at the lowest level so we could reuse - rewind?
//     * can't really do that at the Iterator level, need to do with Iterable
//     * but see Iterator.isTraversableAgain
//     * 
//     */
//  }
  
  private var _iterator: Iterator[SampleData] = null
  
  def iterator: Iterator[SampleData] = _iterator match {
    case null => (_domain.iterator zip _range.iterator).map(p => SampleData(p._1,p._2))
    case _ => _iterator
  }
    //(domainSet.iterator zip rangeData.iterator).map(p => p._1.concat(p._2))
}

object SampledData {
  
  def apply(domainData: IterableData, rangeData: IterableData): SampledData = SampledData(DomainSet(domainData), rangeData)
    
  def apply(domainSet: DomainSet, rangeData: IterableData): SampledData = {
    val sd = new SampledData
    sd._domain = domainSet
    sd._range = rangeData
    sd
  }
  
  //TODO: explore implications of duplicated Iterators (e.g. its caching)
  def apply(sampleIterator: Iterator[SampleData], sampleTemplate: Sample): SampledData = {
    val (dit,rit) = sampleIterator.duplicate
    val dset = DomainSet(IterableData(dit.map(_.domainData), sampleTemplate.domain.getSize))
    val rdata = IterableData(rit.map(_.rangeData), sampleTemplate.range.getSize)
    SampledData(dset, rdata)
  }
  
//  def apply(sampleIterator: Iterator[SampleData]) = {
//    val sd = new SampledData
//    sd._iterator = sampleIterator
//    sd
//  }
}