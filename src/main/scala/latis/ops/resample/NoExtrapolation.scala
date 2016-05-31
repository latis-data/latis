package latis.ops.resample

trait NoExtrapolation extends Extrapolation {
  
  def extrapolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double] = {
    (d: Double) => None
  }
  
}