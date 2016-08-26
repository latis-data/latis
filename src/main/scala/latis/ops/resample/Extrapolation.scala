package latis.ops.resample

import latis.dm.Sample
import latis.dm.Scalar

trait Extrapolation { 
  
  /**
   * Given ordered pairs of x and y, return a function that 
   * when given an x value it will optionally return a corresponding y value.
   */
  def extrapolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double]
  
  def extrapolate(samples: Array[Sample], domain: Scalar): Option[Sample] = ???
  
  //TODO: sliding window size? must be consistent with interp as used in FullOuterJoin
  //combine interp and extrap => resample? 
}