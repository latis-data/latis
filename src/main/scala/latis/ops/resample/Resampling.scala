package latis.ops.resample

import latis.dm.Sample
import latis.dm.Variable

trait Resampling {
  
  /*
   * TODO: 
   * interp: D -> Option[D] = 
   * exterp: D -> Option[D] = 
   * default to error if no exact match
   */

  def resample(samples: Array[Sample], domain: Variable): Some[Sample] = {
    
    
    
    ???
  }
  
  /*
   * add static members that implement "resample"?
   * that can override Function's default resample? mixed in
   * 
   * but at Function or Dataset (monad) level?
   * 
   * Resample has resample(sample1: Sample, sample2: Sample, domain: Variable): Sample
   * don't want to exclude strategies that need more than neighbor samples
   * 
   * use DomainSet? 
   *   indexOf like visad
   *   but can we generally get the DomainSet?
   *     easy for Function with SampledData
   *     seems unfortunate to have to decompose sample iterator
   *     
   * --
   * "Resampling" like Ordering
   *   ordering.compare(x,y): Int
   *   what uses Ordering? SeqLike.sorted
   *   Sorting.quickSort(array)(implicit Ordering)
   * use sliding with iterator to get array of samples
   * use those samples for the core "resample" or "interpolate"? method
   *   or function of doubles?
   * org.apache.commons.math3.analysis.interpolation
   *   univariateInterpolator.interpolate(xs, ys): UnivariateFunction
   *   univariateFunction.value(double): double 
   *     impl as a 1st class function? interpolate: Double -> Double
   *   requires arrays on xs and ys so we can't iterate
   *   could use for each set of sliding samples
   * Interpolating vs Interpolator
   * Interpolation vs Resampling?
   *   "resample" isn't widely used
   *   but don't want to rule out extrapolation
   * *Resampling HAS-A Interpolator?
   *   and maybe an extrapolator?
   *   SampledFunction with XResampling
   *   from Resampling trait: resample(samples: array[Sample], domain: Variable)   Scalar or Tuple
   *   SampledFunction could have resample(domainSet: Seq[V])
   * InterpolatorFactory?
   *   given type, xs, ys 
   *   return function: Double -> Double
   * vs Sampling? ties to statistics
   * 
   * as Operation?
   *   resample
   *   need to be able to mixin with Function
   *   or use implicitly like Ordering???
   *     resample(domainSet)(implicit Resampling)
   *     sorted takes implicit Ordering
   *     can be specified to override default?
   *     how to define implicit Resampling for a Function?
   *     mixin?
   *     does sorted get it from param'd type of List?
   *   impl as Operation
   *   
   * How to mixin different resampling strategy
   *   tsml attribute
   *     mixin: new Function with resampling
   *     with reflection?
   *   implicit def resampling
   *   latis property
   *   
   * smpFunction.resample(domainSet)
   *   iterator.sliding(n)
   * resampling.resample(samples: array[Sample], domain: Variable)
   */
  
/*

trait O {
  implicit def value: String = "o"
}

trait A extends O {
  override implicit def value = "a"
}

trait B extends O {
  override implicit def value = "b"
}

class C extends O { //e.g. Function with NoResampling
//  def write(): Unit = write
//  def write(implicit value: String) = println(value)
  def foo(n: Int): Unit = write(n)
  def write(n: Int)(implicit value: String) = println(value) 
}

object ImplicitFromTrait extends App {
  //implicit def value = "c"
  val c = new C with B  //e.g. Function with LinearResampling
  c.write(0)("z")
  c.foo(0) //can't use same name
  
}
  
*/
}