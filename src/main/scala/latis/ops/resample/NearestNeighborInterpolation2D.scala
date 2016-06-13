package latis.ops.resample

/**
 * Interpolates a value to whichever given value it is closest to. 
 * If equidistant to two points, it will return the lower point. 
 */
trait NearestNeighborInterpolation2D extends Interpolation {
  
  def interpolator(domain: Array[Array[Double]], range: Array[Double]) = (x: Double, y: Double) => {

  }
  
}
