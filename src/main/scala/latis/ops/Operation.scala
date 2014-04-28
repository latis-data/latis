package latis.ops

import latis.dm._
import latis.util.LatisProperties

trait Operation {

  /**
   * Apply this Operation to the given Dataset.
   */
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.getVariables.flatMap(applyToVariable(_)))
    //TODO: provenance metadata
  }
  
  /**
   * Apply Operation to a Variable.
   */
  def applyToVariable(variable: Variable): Option[Variable] = variable match {
    case scalar: Scalar     => applyToScalar(scalar)
    case sample: Sample     => applyToSample(sample)
    case tuple: Tuple       => applyToTuple(tuple)
    case function: Function => applyToFunction(function)
  }

  /**
   * Default no-op operation for Scalars.
   */
  def applyToScalar(scalar: Scalar): Option[Variable] = Some(scalar)
  
  /**
   * Default operation for Samples. Apply operation to the range, keeping the same domain.
   * If the resulting range is invalid, the whole sample is invalid.
   */
  def applyToSample(sample: Sample): Option[Variable] = {
    //return Var since ops like reduce can change type
    applyToVariable(sample.range) match {
      case Some(r) => Some(Sample(sample.domain, r))
      case None => None
    }
  }
  
  /**
   * Default operation for Tuples. Apply operation to each element.
   * If all elements are invalid, then the Tuple is invalid.
   */
  def applyToTuple(tuple: Tuple): Option[Variable] = {
    val vars = tuple.getVariables.flatMap(applyToVariable(_))
    if (vars.length == 0) None
    else Some(Tuple(vars))
  }
  
  /**
   * Default operation for a Function. Wrap the original Function Apply operation to each sample.
   */
  def applyToFunction(function: Function): Option[Variable] = this match {
    case op: SampleMappingOperation => Some(WrappedFunction(function, op))
    //case homo: SampleHomomorphism => Some(WrappedFunction(function, homo))
    case _ => throw new UnsupportedOperationException("Only SampleMappingOperations can use the default Function application.")
  }
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
     * always use Option: invalid operation could return None
     *   (1 -> 2,a,b) * 2 => (1 -> 4) ?  a*2 => None?
     * 
     * ***AlgebraicOperation
     *   doesn't affect data
     *   needs to munge model
     *     op.applyToSampleType?
     *     safe to call since it wont access data
     *   Projection?
     *   applied before vs after 2nd construction pass
     *   
     * ++should WrappedFunction test Op type and invoke as needed?
     *   if algebraic munge type
     * what about diff Iterator types?
     * 
     * 
     * MungedFunction (Filter, Transform)
     * need to count on Sample remaining a Sample for iteration
     * 3 kinds of munging?
     *   Filter may drop sample
     *   sample preserving: modified, morphed, ...?
     *     transform implies changing form
     *   all bets off, maybe not even iterable
     * Projection: may need to fill with Index, not sure if we can do that with generic TransformedFunction
     * 
     * Ops extend SampleApplicable trait
     *   applySample(sample: Sample): Option[Sample]
     * 
     * isomorphic: same type, has inverse
     * homomorphic: same type
     * 
     * should we be able to select/filter on index even if domain is projected?
     * like hyperslab
     * implicit coord system transform between index and domain
     * (index <-> time) -> foo
     * 
     */
