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

  /**
   * Gets the interpolator for a scalar function of Numbers. Otherwise defaults
   * a NaN interpolator. 
   */
  def getInterpolator(function: Function): Double => Option[Double] = {
    val samples = function.iterator.toArray
    samples.head match {
      case Sample(d: Number, r: Number) => {
        val xs = samples.map(_.domain.getNumberData.doubleValue)
        val ys = samples.map(_.range.getNumberData.doubleValue)
        interpolator(xs,ys)
      }
      case _ => (d: Double) => Some(Double.NaN)
    }
  }
}
