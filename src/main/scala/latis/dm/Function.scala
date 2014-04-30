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
  
  def iterator: Iterator[Sample]
  //TODO: only applicable to SampledFunction, need to replace lots of pattern matches...
  
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
  
  def apply(domain: Variable, range: Variable, md: Metadata = EmptyMetadata, data: SampledData = EmptyData): Function = new SampledFunction(domain, range, md, data)
  
  //TODO: do we have a use case to construct with a set of Samples as opposed to data values?
  //  LimitFilter
//  def apply(domain: Variable, range: Variable, sampleIterator: Iterator[Sample], md: Metadata = EmptyMetadata): Function = {
//    val data = SampledData(sampleIterator, Sample(domain, range))
//    ???
//  }
  
//TODO: make sure samples are sorted!
  
//  def apply(vs: Seq[Variable], metadata: Metadata = EmptyMetadata): Function = vs.head match {
//    case sample: Sample => Function(sample.domain, sample.range, vs.asInstanceOf[Seq[Sample]].iterator, metadata)
//    case _ => {
//      //make Seq of samples where domain is index
//      val samples = vs.zipWithIndex.map(s => Sample(Index(s._2), s._1))
//      val sample = samples.head
//      Function(sample.domain, sample.range, samples.iterator, metadata)
//    }
//  }
  
//  def apply(ds: Seq[Variable], rs: Seq[Variable]): Function = {
//    //TODO: assert same length?
//    Function((ds zip rs).map(s => Sample(s._1, s._2)))
//  }
  
  //TODO: names arg
  def fromValues(vals: Seq[Seq[Double]]): Function = Function.fromValues(vals.head, vals.tail: _*)
  
  def fromValues(dvals: Seq[Double], vals: Seq[Double]*): Function = {
    val domain = Real(Metadata("domain"))
    val range = vals.length match {
      case 1 => Real(Metadata("range"))
      case n: Int => Tuple((0 until n).map(i => Real(Metadata("real"+i)))) //auto-gen names
    }
    val data: SampledData = {
      //assert that all Seq are same length
      if (vals.exists(_.length != dvals.length)) throw new Error("Value sequences must be the same length.")
      //vals.foldLeft(Data.fromDoubles(dvals: _*))(_ zip Data.fromDoubles(_: _*))
      val rdata = vals.tail.foldLeft(Data.fromDoubles(vals.head: _*))(_ zip Data.fromDoubles(_: _*))
      SampledData(DomainSet(Data.fromDoubles(dvals: _*)), rdata)
      //TODO: SampledData.fromValues?
    }
    
    //make Real if vals.length == 1
    //val range = if (vals.length == 1) Real(Metadata("range"), vals(0)) else Tuple(vals)(0d) //hack to get around type erasure ambiguity
    Function(domain, range, data = data)
  }
  
  //def apply(samples: Seq[Sample]): Function = 
  
  def unapply(f: Function): Option[Iterator[Sample]] = Some(f.iterator)
  
}



  
