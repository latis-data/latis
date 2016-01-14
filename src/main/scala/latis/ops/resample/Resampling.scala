package latis.ops.resample

import latis.data.Data
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Number
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.implicits.variableToDataset
import latis.ops.Operation
import latis.ops.Split
import latis.ops.agg.BasicJoin
import latis.time.Time

class Resampling(domainSet: Iterable[Variable]) extends Operation with NoInterpolation {
  /*
   * TODO: construct with DomainSet?
   * currently DomainSet is just Data
   * need to be able to match type, name?
   *   convert units...
   *   add metadata "template" to it?
   *   def apply(index: Int): Data
   *   def indexOf(data: Data): Int
   * needs to be iterable
   * 
   *   
   */
  
  val dname = domainSet.head.getName
   

  override def applyToFunction(function: Function): Option[Function] = {
    val newSamples = if(function.getDomain.hasName(dname)) { 
      val dvals = domainSet.head match { //only 1D for now
        case _: Number => domainSet.map(_.getNumberData.doubleValue)
      }
      function.getRange match {
        case r: Scalar => {
          val inpr = getInterpolator(function)
          val rvals = dvals.flatMap(inpr(_))
          domainSet.zip(rvals).map(p => Sample(p._1, r(Data(p._2))))
        }
        case t: Tuple => { //apply resampling to each variable in the tuple individually
          val fs = Split()(function) match {
            case Dataset(Tuple(vs)) => vs.flatMap(_ match {
              case f: Function => applyToFunction(f)
              case _ => None
            })
          }
          fs.foldLeft(Dataset.empty)(BasicJoin(_,_)) match {
            case Dataset(f: Function) => f.iterator.toSeq
          }
        }
      }
    }
    else {
      function.iterator.flatMap(applyToSample(_))
    }

    Some(Function(newSamples.toSeq, function.getMetadata + ("length" -> newSamples.size.toString)))
  }
//  
//  /**
//   * Split a Function with a Tuple range into a Seq of scalar Functions, one 
//   * for each Variable in the Tuple.
//   */
//  def split(function: Function): Seq[Function] = function.getRange match {
//    case Tuple(vars) => {
//      val samples = function.iterator.toSeq
//      val dom = samples.map(_.domain)
//      val ran = samples.map(_.range.asInstanceOf[Tuple].getVariables).transpose
//      ran.map(Function(dom, _))
//    }
//  }
//  
//  /**
//   * Combine a Seq of Functions into a single Function by wrapping the ranges of
//   * each individual Function into a single Tuple. Assumes each function has the 
//   * same domain.
//   */
//  def splice(fs: Seq[Function]): Function = {
//    val samples = fs.map(_.iterator.toSeq)
//    val dom = samples(0).map(_.domain)
//    val ran = samples.map(_.map(_.range)).transpose
//    val newSamples = dom.zip(ran).map(p => Sample(p._1, Tuple(p._2)))
//    Function(newSamples)
//  }
//  
  /**
   * Gets the interpolator for a scalar function of Numbers. Otherwise defaults
   * a NaN interpolator. 
   */
  def getInterpolator(function: Function): Double => Option[Double] = {
    val samples = function.iterator.toArray
    samples(0) match {
      case Sample(d: Number, r: Number) => {
        val xs = samples.map(_.domain.getNumberData.doubleValue)
        val ys = samples.map(_.range.getNumberData.doubleValue)
        interpolator(xs,ys)
      }
      case Sample(t: Time, r: Number) => {
        val xs = samples.map(_.domain.asInstanceOf[Time].getJavaTime.doubleValue)
        val ys = samples.map(_.range.getNumberData.doubleValue)
        interpolator(xs,ys)
      }
      case _ => (d: Double) => Some(Double.NaN)
    }
  }

}
