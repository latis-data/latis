package latis.ops.resample

trait Interpolation { 
  
  def interpolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double]
  
}