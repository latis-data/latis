package latis.ops.resample

import latis.dm.Sample
import latis.dm.Scalar
import latis.data.Data

trait NoExtrapolation extends Extrapolation {
  
  def extrapolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double] = {
    (d: Double) => None
  }
  
  //TODO: generalize use of extrapolator function (see Interpolation)
  override def extrapolate(samples: Array[Sample], domain: Scalar): Option[Sample] = {
    //copy range scalar with fill value
    val range = samples.head.range match {
      case s: Scalar => s(Data(s.getFillValue))
      case _ => throw new UnsupportedOperationException("Extrapolation requires a Scalar domain for now.")
    }
    
    Some(Sample(domain, range))
  }
}