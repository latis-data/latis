package latis.ops.resample

import latis.dm.Sample
import latis.dm.Scalar

trait Interpolation { 
  
  /**
   * Given ordered pairs of x and y, return a function that 
   * when given an x value it will optionally return a corresponding y value.
   */
  def interpolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double]
  
  def interpolate(samples: Array[Sample], domain: Scalar): Option[Sample] = ???
  
  /**
   * Number of samples needed to perform an interpolation
   * assuming the interpolate is in the middle?
   * Default to 2. Can be overridden. Should be even.
   */
  val interpolationWindowSize: Int = 2
}