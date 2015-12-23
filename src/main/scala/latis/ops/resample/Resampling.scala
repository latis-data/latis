package latis.ops.resample

import latis.dm.Sample
import latis.dm.Variable
import latis.ops.Operation
import latis.dm.Function

class Resampling extends Operation with NoInterpolation {
  /*
   * TODO: construct with DomainSet?
   * currently DomainSet is just Data
   * need to be able to match type, name?
   *   convert units...
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   * 
   */
   

  override def applyToFunction(function: Function): Option[Variable] = {
    
    ???
  }

  def resample(samples: Array[Sample], domain: Variable): Some[Sample] = {
    //TODO: make sure domains are consistent, unit conversion...
    domain match {
      case _ => ???
    }

    ???
  }

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
   * impl as Operation
   *   given ds, find Function with matching domain
   *   construct with new domain set and optional Resampling
   *   f2 = f.resample... OR applyToFunction 
   *     eventually delegate to Resampling
   *   Consider how other operations have class defined in properties
   *     operation.resample.class = ?
   *     no dataset granularity
   *    *are there other operations like this? 
   *       integration
   *     order of precedence:
   *       specified optional param
   *       mixed in resampling (tsml or reader.myds.resampling property)
   *       operation.resample.class
   *   
   * How to mixin different resampling strategy
   *   tsml attribute
   *     mixin: new Function with resampling
   *     with reflection?
   *   implicit def resampling
   *   latis property
   * need better reconstruction to preserve mixin 
   *   like case class copy
   *   use reflection to avoid hard-coding mixin options?
   * is mixin the best way to deal with resampling strategy?
   *   composition: HAS-A?
   *   dependency injection
   *   
   * sampledFunction.resample(domainSet) +optional resampling
   *   *this only be done as an operation on a dataset?
   *   iterator.sliding(n), or later? probably later since the interp algorithm knows sliding window size
   *   continuousFunction.sample(domainSet)? or same resample? would presumably use eval/apply, no interp
   * resampling.resample(samples: array[Sample], domain: Variable) 
   *   note: doesn't rely on state like an Operation might
   *   in terms of latis structures
   *   do we need that distinction at this level?
   *     most interp will use doubles but we might want to support Text or Integer resampling
   *   could we reuse the interp function for multiple resamplings?
   *     same reason commons-math interpolate returns a function
   *   should this take the full iterator of samples (apply sliding here)?
   *     or even take Function as arg?
   *     domain or domainSet?
   *     
   * Resampling traits provide interpolation and extrapolation functions
   *   built from xs:Array[Double], ys:Array[Double], return f: Double -> Double
   *     nD domains? require product (Cartesian?) sets for now?
   *     be consistent with commons-math: BivariateGridInterpolator.interpolate(double[] xval, double[] yval, double[][] fval): BivariateFunction
   * 
   * Do we need a "resampling" abstraction? YES
   *   we can "evaluate" a LaTiS Function
   *   one sample vs a domainSet, optimization possibility only?   
   *   resampling on a set (must be ordered) can be streamed
   *   general evaluation can't depend on a stream  
   * 
   * trait with self type annotations
   *   trait FooResampling {this: BarInterp with BazExtrap => ... }
   * Interpolation trait
   *   impl org.apache.commons.math3.analysis.interpolation.UnivariateInterpolator ?
   *     interpolate(double[] xval, double[] yval): UnivariateFunction
   *   use commons-math Interpolator instances
   *     as a member of our trait, delegate to its interpolate
   *   interpolator(xs: Array[Double], ys: Array[Double]): Double -> Double
   *   what about tuple ranges, need iterator for each
   *     can invoke interpolator for each
   *   not to mention that it will need to be invoked for each sliding window
   *   
   * Who should do the sliding?
   *   the Resampling trait should know
   *     use sliding on function's iterator
   *     duplication in extracting values from samples but more idiomatic
   *     otherwise need to manage a "cache" of recent data values
   *   the Interpolation trait will be used for each window
   */
  
    /*
   * TODO: 
   * interp: D -> Option[D] = 
   * exterp: D -> Option[D] = 
   * default to error if no exact match
   */
  
  

  
/*
trait inheritance tinkering from ScalaTest on kestrel

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