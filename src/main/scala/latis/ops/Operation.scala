package latis.ops

import latis.dm.Dataset
import latis.util.LatisProperties

trait Operation {

  def apply(dataset: Dataset): Dataset

}

object Operation {
    
  /**
   * Construct an Operation subclass based on the given name.
   */
  def apply(opName: String): Operation = apply(opName, Seq[String]())

  /**
   * Construct an Operation subclass based on the given name and arguments.
   */
  def apply(opName: String, args: Seq[String]): Operation = {
    try { 
      val compObj = getCompanionObject(opName)
      if (args.length == 0 || args.head.length == 0) compObj.apply() //no args or empty first arg
      else compObj.apply(args)
    } catch {
      case e: Exception => throw new UnsupportedOperationException(opName + ": " + e.getMessage)
    }
  }

  /**
   * Get the class of the desired Operation subclass.
   * The class name must be defined as a property of the form "operation.<opName>.class".
   */
  private def getClassFromOpName(opName: String) = {
    LatisProperties.get("operation." + opName + ".class") match {
      case Some(cname) => Class.forName(cname) //TODO: handle ClassNotFoundException?
      case None => throw new UnsupportedOperationException("No Operation class defined for: " + opName)
    }
  }
    
  /**
   * Use reflection to get the companion object of the desired Operation subclass.
   */
  private def getCompanionObject(opName: String): OperationFactory = {
    import scala.reflect.runtime.currentMirror
    
    val cls = getClassFromOpName(opName)
    val moduleSymbol = currentMirror.classSymbol(cls).companionSymbol.asModule
    currentMirror.reflectModule(moduleSymbol).instance.asInstanceOf[OperationFactory] 
  }
    
}
//    
//    //type not necessarily preserved
//    protected def applyToScalar(scalar: Scalar): Variable = scalar //no-op
//    protected def applyToTuple(tuple: Tuple): Variable = Tuple(tuple.variables.map(applyToVariable(_)))
//    protected def applyToFunction(function: Function): Variable = WrappedFunction(function, this)
//      //TODO: or Function(applyToVariable(function.domain), applyToVariable(function.range)) ?
//      
//    //typically invoked by WrappedFunction
//    protected def applyToSample(sample: Sample): Variable = 
//      Sample(applyToVariable(sample.domain), applyToVariable(sample.range))

    /*
     * Operations: subclass of Operation for each? easier to reason about
     *   Selection: exclude Function samples, affect only provenance metadata
     *   Projection: exclude Variables from the model, affect only provenance metadata
     *   Transformation
     *     ++need a type of op that doesn't change type/model?
     *       e.g. unit conversion, basic math
     *       change values (and metadata)
     *       do we need a subclass? easier to reason about?
     *     traits for ModelChanging, DataChanging, ...
     *   ("Rename" is also part of relational algebra)
     * 
     * Things that can change in a Dataset operation
     * - model, same values
     *     projection
     *     reduction
     *     factorization
     * - model, diff values
     *     derived field, FFT, ...
     * - number of samples, same values
     *     filter
     *     selection
     *     limit
     *     subset (without reduction: removing dimensions of length 1)
     * - number of samples, same values, diff model (reduced dimensions)
     *     slice (subset + reduce)
     *     first, last, min, max (filter + reduce)
     * - number of samples, diff values, diff model (reduced dimensions)
     *     integrate
     * - number of samples, diff values
     *     resample
     *     bin 
     * - data values, same number of samples
     *     basic math
     *     unit conversion
     *     replace
     * - metadata only
     *     rename
     *     enhance/repair
     * aggregation
     * 
     * pivot
     * coordinate transform
     * evaluate
     * op on domain vs range
     * 
     * --
     * Filter: return Option
     * Transform: return Var
     * 
     */
