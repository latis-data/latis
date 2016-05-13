package latis.ops

import latis.dm._

class Evaluation extends Operation {
  
  /*
   * TODO: need to know which function to apply this to, by domain var name?
   * 
   * function.apply(arg, strategy)
   *   strategy optional, otherwise use the one tied to the function?
   *   but can only apply operation to dataset
   * 
   * should the resampling/interpolation strategy be a property/metadata of the Dataset?
   * 
   * think of function evaluation like Map access by key with interpolation (and extrapolation) mixed in
   *   f:F[D,C] 
   *   f(d:D): C
   * need to be able to interpolate data of type C (Interpolation[C]?)
   *   akin to Ordering
   *   and likewise for D?
   *   probably just need to support interp for any Variable
   * how should this operate of Scalars and Tuples?
   *   should it even be an Operation?
   *   part of Resampling or such?
   *   do we really ever want to get just one new sample back as a dataset  
   */
  
  override def applyToFunction(function: Function): Option[Variable] = {
  
    
    ???
  }
}