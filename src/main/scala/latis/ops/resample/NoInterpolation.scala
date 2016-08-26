package latis.ops.resample

trait NoInterpolation extends Interpolation { 
  
  def interpolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double] = {
    //TODO: assert xs.length == ys.length
    (d: Double) => xs.indexOf(d) match {
      case -1 => None
      case index: Int => Some(ys(index))
    }
  }
  
}