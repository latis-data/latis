package latis.ops.resample

import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Number
import latis.data.value.DoubleValue
import latis.data.Data
import latis.dm.Dataset

trait Interpolation { 
  
  /**
   * Given ordered pairs of x and y, return a function that 
   * when given an x value it will optionally return a corresponding y value.
   */
  def interpolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double]
  
  def interpolate(samples: Array[Sample], domain: Scalar): Option[Sample] = {
    //assume Samples of type Number -> Number
    
    //x and y arrays
    val xys: (Array[Double], Array[Double]) = samples.map(s => s match {
      case Sample(Number(x), Number(y)) => (x,y)
    }).unzip
    
    //interpolate value
    val x: Double = domain match {case Number(x) => x}
    
    //template for constructing a range variable
    val temp = samples.head.range match {
      case n: Scalar => n
      case _ => throw new UnsupportedOperationException("Interpolation supports only numeric Scalar ranges for now.")
    }
    
    //create and invoke an interpolator
    val range = interpolator(xys._1, xys._2)(x) match {
      case Some(y) => temp(Data(y))
      case None => temp(Data(temp.getFillValue))
    }
    
    Some(Sample(domain, range))
  }
  
  /**
   * Number of samples needed to perform an interpolation
   * assuming the interpolate is in the middle?
   * Default to 2. Can be overridden. Should be even.
   */
  val interpolationWindowSize: Int = 2
}