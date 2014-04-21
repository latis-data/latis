package latis.dm

import scala.collection._
import latis.data._
import latis.metadata._
import java.nio.ByteBuffer

trait Function extends Variable { //this: Variable =>
  def getDomain: Variable
  def getRange: Variable
  
  //evaluate for given domain sample
  //def apply(v: Variable): Variable
  
  //TODO: reconsider when we add ContinuousFunction
  def getFirstSample: Sample
  def getLastSample: Sample
  def getSample(index: Int): Sample
  
  //TODO: put in Variable?
  //def length: Int  getLength?
  def iterator: Iterator[Sample]
}


object Function {
  //TODO: used named args for data, md?
  
  
  def apply(domain: Variable, range: Variable): Function = new SampledFunction(domain, range)
  
  def apply(domain: Variable, range: Variable, md: Metadata): Function = new SampledFunction(domain, range, metadata = md)
  
  def apply(domain: Variable, range: Variable, data: Data): Function = new SampledFunction(domain, range, data = data)

  
  //build from Iterator[Sample]
  //TODO: could we set data to something?
  def apply(domain: Variable, range: Variable, sampleIterator: Iterator[Sample], md: Metadata): Function = {
    //Note, wouldn't need domain and range, but would have to trigger iterator, or use peek?
    new SampledFunction(domain, range, sampleIterator, metadata = md)
  }
  
  def apply(domain: Variable, range: Variable, sampleIterator: Iterator[Sample]): Function = 
    Function(domain, range, sampleIterator, EmptyMetadata)
  
  def apply(domain: Variable, range: Variable, md: Metadata, data: Data): Function = {
    new SampledFunction(domain, range, metadata = md, data = data)
  }
  
//TODO: make sure samples are sorted!
  
  def apply(vs: Seq[Variable], metadata: Metadata = EmptyMetadata): Function = vs.head match {
    case sample: Sample => Function(sample.domain, sample.range, vs.asInstanceOf[Seq[Sample]].iterator, metadata)
    case _ => {
      //make Seq of samples where domain is index
      val samples = vs.zipWithIndex.map(s => Sample(Index(s._2), s._1))
      val sample = samples.head
      Function(sample.domain, sample.range, samples.iterator, metadata)
    }
  }
  
  def apply(ds: Seq[Variable], rs: Seq[Variable]): Function = {
    //TODO: assert same length?
    Function((ds zip rs).map(s => Sample(s._1, s._2)))
  }
  
  def fromValues(vals: Seq[Seq[Double]]): Function = Function.fromValues(vals.head, vals.tail: _*)
  
  def fromValues(dvals: Seq[Double], vals: Seq[Double]*): Function = {
    val domain = Real(Metadata("domain"), dvals)
    //make Real if vals.length == 1
    val range = if (vals.length == 1) Real(Metadata("range"), vals(0)) else Tuple(vals)(0d) //hack to get around type erasure ambiguity
    Function(domain, range)
  }
  
  //def apply(samples: Seq[Sample]): Function = 
  
  def unapply(f: Function): Option[(Variable, Variable)] = Some((f.getDomain, f.getRange))
  //TODO: expose Seq[Samples]?
  
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