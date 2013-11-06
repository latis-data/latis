package latis.dm

import scala.collection._
import latis.data._
import latis.metadata._
import java.nio.ByteBuffer
import latis.util.Util

trait Function extends Variable { //this: Variable =>
  def getDomain: Variable
  def getRange: Variable
  
  //TODO: reconsider when we add ContinuousFunction
  def getFirstSample: Sample
  def getLastSample: Sample
  
  //TODO: put in Variable?
  //def length: Int  getLength?
  def iterator: Iterator[Sample]
}

class SampledFunction(domain: Variable, range: Variable, _iterator: Iterator[Sample] = null,
    metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
    extends Variable2(metadata, data) with Function {
  
  /*
   * 2013-08-07
   * TODO: ContinuousFunction
   * now that Data is separable, how can we support continuous function (e.g. exp model)
   * ContinuousFunction:
   *   apply(domainVal: Variable) => range val
   *   apply(domainSet: Seq[Variable] or Var with SeqData) => SampledFunction
   *   length = -1?
   *   iterator => error
   */

  //expose domain and range via defs only so we can override (e.g. ProjectedFunction)
  def getDomain: Variable = domain
  def getRange: Variable = range
  
  //private var _iterator: Iterator[Sample] = null
  
  def iterator: Iterator[Sample] = {
    if (_iterator != null) _iterator
    else if (getData.isEmpty) iterateFromKids
    else getDataIterator.map(Util.dataToSample(_, Sample(domain, range)))
  }
  
  private def iterateFromKids: Iterator[Sample] = {
    val dit = domain.getDataIterator.map(data => Util.dataToVariable(data, domain))
    val rit = range.getDataIterator.map(data => Util.dataToVariable(data, range))
    (dit zip rit).map(pair => Sample(pair._1, pair._2))
  }

  
  //Support first and last filters
  //TODO: consider more optimal approaches
  def getFirstSample: Sample = iterator.next
  def getLastSample: Sample = {
    //iterator.drop(length-1).next  //dataIterator is giving Util.dataToSample null Data!?
    
    //TODO: only gets 190 of 570!?
    //trying to get new iterator with each ref? skipping by 3s
//    while(iterator.hasNext) {
//      sample = iterator.next
//      c = c+1
//      println(c +": "+ sample.domain.data)
//    }
    
    var sample: Sample = null
    for (s <- iterator) sample = s
    sample 
  }
  //TODO: return Function with single sample to preserve namespace...?

}

object Function {
  //TODO: used named args for data, md?
  
  
  def apply(domain: Variable, range: Variable): Function = new SampledFunction(domain, range)
  
  def apply(domain: Variable, range: Variable, md: Metadata): Function = new SampledFunction(domain, range, metadata = md)
  
  def apply(domain: Variable, range: Variable, data: Data): Function = new SampledFunction(domain, range, data = data)

  
  //build from Iterator[Sample]
  //TODO: could we set data to something?
  def apply(domain: Variable, range: Variable, sampleIterator: Iterator[Sample]): Function = {
    //Note, wouldn't need domain and range, but would have to trigger iterator, or use peek?
    new SampledFunction(domain, range, sampleIterator)
  }
  
  def apply(domain: Variable, range: Variable, md: Metadata, data: Data): Function = {
    new SampledFunction(domain, range, metadata = md, data = data)
  }
  
  def apply(vals: Seq[Seq[Double]]): Function = Function(vals.head, vals.tail: _*)
  
  def apply(dvals: Seq[Double], vals: Seq[Double]*): Function = {
    val domain = Real(dvals)
    //make Real if vals.length == 1
    val range = if (vals.length == 1) Real(vals(0)) else Tuple(vals)(0d) //hack to get around type erasure ambiguity
    Function(domain, range)
  }
  
  //def apply(samples: Seq[Sample]): Function = 
  
  def unapply(f: Function): Option[(Variable, Variable)] = Some((f.getDomain, f.getRange))
  
}

//  def apply(dvals: Seq[Double], rvals: Seq[Double]): Function = {
//    /*
//     * TODO: store in Data
//     * expose with iterator, instead of delegating to kids
//     * do we need FunctionData? 2 arrays, or interleave like tuple?
//     *   otherwise each can be an IndexFunction
//     * Seq of pairs, zip, facilitate iteration over pairs
//     *   generalize beyond a pair of doubles, (or special case?)
//     *   but then need structure to make sense of numbers
//     *   use byte buffer?
//     *   don't forget unzip
//     * 
//     * Is there a case where we can delegate to kids' data?
//     * iterate into nested functions...
//     * only if we can let kids have multiple samples?
//     * or split Function into 2 IndexFunctions
//     *   not conducive for function iterator
//     * If domain and range scalars can have multiple values in data
//     *   should we expose IndexFunctions in unapply?
//     * 
//     * DomainSet
//     *   as Scalar with multiple values?
//     *   as IndexFunction?
//     *   *as Data?
//     *   could be linear, index math
//     *   RealSet?
//     * RangeSeq, VariableSeq
//     *   could be nested Fs..., can't be Data?
//     *   are these structures needed?
//     *   just a Variable with Seq of Data?
//     * 
//     * Seems like we need to be able to store multiple values in the Data of a Scalar type
//     * In the context of iterating (e.g. math), need to assume that a Scalar has a single value.
//     * This seems to be the same old problem of mixed abstractions.
//     * Can we just use IndexFunction interchangeably when we need such behavior?
//     * 
//     * just put into function data for now
//     * if math see f data, iterate
//     */
//    val domain = Real(dvals)
//    val range = Real(rvals)
//    Function(domain, range)
//  }
  
//
//object IndexFunction {
//  
//  def apply(ds: Array[Double]): IndexFunction = {
//    //TODO: add length to metadata?
//    
//  }
//}