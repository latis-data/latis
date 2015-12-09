package latis.ops

import latis.dm.Function
import latis.dm.Variable
import latis.dm.Sample


/**
 * Fully realize the Data within the Scalars of the Dataset so it no longer has ties to the data source.
 * Useful for smaller Datasets that we want to read then close the Reader.
 * This will return a new Dataset that is logically equivalent.
 */
class Memoization extends Operation {

  /*
   * should only need to realize lazy data: Function iterator
   * presumably only the outer function since that is where iterative adapters are lazy
   * or should we traverse the whole thing as is we are writing it?
   * do we need to worry about internal functions being traverable once?
   */
  override def applyToFunction(function: Function): Option[Variable] = {
    val md = function.getMetadata
    val samples = function.iterator.toList
//    .map(s => s match {
//      //recurse into inner functions //TODO: needed?
//      case Sample(d, f: Function) => applyToFunction(f) match {
//        case Some(f2) => Sample(d, f2)
//        case None => ???
//      }
//      case _ => s
//    })
    //TODO consider IndexedSeq (Vector)? index tied to DomainSet
    
    //Add 'length' to the Function Metadata
    val md2 = md + ("length" -> samples.length.toString)
    
    Some(Function(samples, md2))
  }
  //TODO: deal with nested Functions? Or is only the outer Function allowed to be built with an iterator.
  
  /*
   * TODO: how will this effect Functions with SampledData?
   * in what cases to we build SampledData from iterable data source?
   *   it is used by IterativeAdapter
   * memoize will realize these in the scalars, so need to apply whether SampledData is realized or not
   * there are other issues where we'd like to sustain SampledData (to take advantage of a DomainSet)
   * no need if function isTraversableAgain
   * revisit optimizations another time
   */
}

object Memoization {
  def apply() = new Memoization()
}