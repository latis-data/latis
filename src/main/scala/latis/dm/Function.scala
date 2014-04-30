package latis.dm

import scala.collection._
import latis.data._
import latis.data.set.DomainSet
import latis.metadata._
import java.nio.ByteBuffer

trait Function extends Variable {
  def getDomain: Variable
  def getRange: Variable
  
  //evaluate for given domain sample
  //TODO: delegate to Operation
  //def apply(v: Variable): Variable
  
  //TODO: only applicable to SampledFunction, need to replace lots of pattern matches...
  def iterator: Iterator[Sample]
  def getDataIterator: Iterator[SampleData]
}
  
  /*
   * TODO: ContinuousFunction:
   *   apply(domainVal: Variable) => range val
   *   apply(domainSet: Seq[Variable] or Var with SeqData) => SampledFunction
   *   length = -1?
   *   iterator => error
   */

object Function {
  //TODO: make sure samples are sorted!
  
  def apply(domain: Variable, range: Variable, md: Metadata = EmptyMetadata, data: SampledData = EmptyData): SampledFunction = new SampledFunction(domain, range, md, data)
  
  /**
   * Construct from Iterator of Samples.
   */
  def apply(domain: Variable, range: Variable, sampleIterator: Iterator[Sample], md: Metadata): SampledFunction = {
    SampledFunction(domain, range, sampleIterator, md)
  }
  def apply(domain: Variable, range: Variable, sampleIterator: Iterator[Sample]): SampledFunction = {
    SampledFunction(domain, range, sampleIterator, EmptyMetadata)
  }
  
  /**
   * Construct from Seq of Variable which are assumed to contain their own data.
   */
  //def apply(vs: Seq[Variable], md: Metadata = EmptyMetadata): Function = vs.head match {
  def apply(vs: Seq[Variable]): SampledFunction = vs.head match {
    case sample: Sample => Function(sample.domain, sample.range, vs.asInstanceOf[Seq[Sample]].iterator)
    case _ => {
      //make Seq of samples where domain is index
      //TODO: make sure every Variable in the Seq has the same type
      //TODO: make from SampledData with IndexSet
      val samples = vs.zipWithIndex.map(s => Sample(Index(s._2), s._1))
      val sample = samples.head
      Function(sample.domain, sample.range, samples.iterator)
    }
  }
  
  /**
   * Construct from a Seq of domain Variables and a Seq of range Variables.
   */
  def apply(ds: Seq[Variable], rs: Seq[Variable]): SampledFunction = {
    if (ds.length != rs.length) throw new Error("Domain and range sequences must have the same length.")
    Function((ds zip rs).map(s => Sample(s._1, s._2)))
  }
  
  /**
   * Construct from a 2D sequence of double values. Assume the first is for a 1D domain.
   */
  def fromValues(vals: Seq[Seq[Double]]): SampledFunction = Function.fromValues(vals.head, vals.tail: _*)
    
  /**
   * Construct from a sequence of double values and a sequence of range values for multiple variables.
   */
  def fromValues(dvals: Seq[Double], vals: Seq[Double]*): SampledFunction = {
    val domain = Real(Metadata("domain"))
    val range = vals.length match {
      case 1 => Real(Metadata("range"))
      case n: Int => Tuple((0 until n).map(i => Real(Metadata("real"+i)))) //auto-gen names
    }
    val data: SampledData = {
      //assert that all Seq are same length as the domain
      if (vals.exists(_.length != dvals.length)) throw new Error("Value sequences must be the same length.")
      val rdata = vals.tail.foldLeft(Data.fromDoubles(vals.head: _*))(_ zip Data.fromDoubles(_: _*))
      SampledData(DomainSet(Data.fromDoubles(dvals: _*)), rdata)
      //TODO: SampledData.fromValues?
    }
    
    Function(domain, range, data = data)
  }
  
  /**
   * Expose the Sample Iterator.
   */
  def unapply(f: SampledFunction): Option[Iterator[Sample]] = Some(f.iterator)
  //TODO: only applies to SampledFunction, define it there
  
}



  
