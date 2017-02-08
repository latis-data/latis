package latis.ops

import latis.dm._

class Evaluation(value: Variable) extends Operation {
  
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
   *     monadic, flatMap? f: V -> D[V]
   * eval takes a Function of A -> B to a Variable of type B
   * seems like we only want to evaluate a Function
   *   a resampling will call for each sample 
   * the DSL can allow eval of a Dataset wrapping a Function
   * but we should be able to use the Function eval as needed?
   * but eval will call for recursion like operation, if we interpolate? or does recursion happen in interp op
   *   
   * 
   * 
   */
  
  //TODO: error if Dataset has more than Function
  //TODO: make sure dataset is memoized so we can use more than once; but avoid doing it every time
  //TODO: make sure value is compatible with Function domain: units, convertible...
  
  override def applyToFunction(function: Function): Option[Variable] = {
    
    
    ???
  }
}

object Evaluation {
  
  def apply(value: Variable) = new Evaluation(value)
}