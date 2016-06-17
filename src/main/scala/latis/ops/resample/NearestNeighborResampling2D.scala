package latis.ops.resample

import latis.dm._

class NearestNeighborResampling2D(domainSet: Iterable[Variable]) 
  extends Resampling(domainSet) with NearestNeighborInterpolation2D {

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

  override def interpolator(domain: Array[Array[Double]], range: Array[Double]) =
    super[NearestNeighborInterpolation2D].interpolator(domain, range)

  def getInterpolator(function: Function): (Double, Double) => Option[Double] = {
    val samples = function.iterator.toArray
    samples.head match {
      case Sample(tv: Tuple, r: Number) => {
        val domain_x = samples.map(_.domain.asInstanceOf[Tuple].getVariables(0).getNumberData.doubleValue)
        val domain_y = samples.map(_.domain.asInstanceOf[Tuple].getVariables(1).getNumberData.doubleValue)
        val domainSet = Array(domain_x, domain_y)
        val rangeSet = samples.map(_.range.getNumberData.doubleValue)
        interpolator(domainSet, rangeSet)
      }
    }
  }
}
