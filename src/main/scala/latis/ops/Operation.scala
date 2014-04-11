package latis.ops

import latis.dm._
import latis.util.LatisProperties

trait Operation {
  
  /*
   * Like scala, we should support something like 
   *   ds.filter(p: Variable => Boolean) 
   *   ds.map(f: Variable => Variable)
   *   
   * Do we need Transformation or just use Operation with Filter as special kind?
   * 
   * Filter super (trait?)
   * ultimately support dataset.filter(predicate)?
   * currently myFilter(dataset)
   * filter(var): Boolean?
   * 
   * bothered by semantics:
   *   operation "operates on" a dataset
   *   apply an operation to a dataset
   *     consistent with operation.apply(ds) or operation(ds)?
   *     
   * Function as morphism (arrow, functor)
   * Operation transforms F to F: Natural Transformation !?
   */
  
  def apply(dataset: Dataset): Dataset

  //  protected def applyToVariable(variable: Variable): Variable = variable match {
  //    case s: Scalar => applyToScalar(s)
  //    case t: Tuple => applyToTuple(t)
  //    case f: Function => applyToFunction(f)
  //  }
  //  
  //  //type not necessarily preserved
  //  protected def applyToScalar(scalar: Scalar): Variable = scalar //no-op
  //  protected def applyToTuple(tuple: Tuple): Variable = Tuple(tuple.variables.map(applyToVariable(_)))
  //  protected def applyToFunction(function: Function): Variable = WrappedFunction(function, this)
  //    //TODO: or Function(applyToVariable(function.domain), applyToVariable(function.range)) ?
  //    
  //  //typically invoked by WrappedFunction
  //  protected def applyToSample(sample: Sample): Variable = 
  //    Sample(applyToVariable(sample.domain), applyToVariable(sample.range))

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
     */

}

object Operation {
  
  def apply(name: String): Operation = Operation(name, List(""))

  def apply(name: String, args: Seq[String]): Operation = {
    LatisProperties.get("operation." + name + ".class") match {
      case Some(cname) => {
       try {
        val cls = Class.forName(cname)
        if (args.head.length == 0) {  //no args
          val ctor = cls.getConstructor()
          ctor.newInstance().asInstanceOf[Operation]
        } else {
          val ctor = cls.getConstructor(classOf[Seq[String]])
          ctor.newInstance(args).asInstanceOf[Operation]
        }
       } catch {
         case e: Exception => throw new RuntimeException("Unsupported Operation: " + name, e)
       }
      }
      case None => throw new RuntimeException("Unsupported Operation: " + name)
    }
  }

}






