package latis.ops

import scala.Option.option2Iterable
import scala.reflect.runtime.currentMirror

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.util.LatisProperties
import latis.util.iterator.MappingIterator

/**
 * Base type for operations that transform on Dataset into another.
 */
abstract class Operation {
  
  /**
   * Apply this Operation to the given Dataset.
   */
  def apply(dataset: Dataset): Dataset = dataset match {
    case Dataset(variable) => {
      val md = dataset.getMetadata
      //TODO: delegate to subclass to munge metadata
      //TODO: add provenance metadata, getProvMsg, append to "history"
      applyToVariable(variable) match {
        case Some(v) => Dataset(v, md)
        case None => Dataset.empty
      }
    }
    case _ => dataset
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
  def applyToSample(sample: Sample): Option[Sample] = {
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
    else Some(Tuple(vars, tuple.getMetadata))
  }
  
  /**
   * Default operation for a Function. Encapsulate with operation in WrappedFunction
   * to be applied to each sample as it iterates.
   */
  def applyToFunction(function: Function): Option[Variable] = {
    val mit = new MappingIterator(function.iterator, (s: Sample) => this.applyToSample(s))
    val template = mit.peek match {
      case null => function.getSample //empty iterator so no-op
      case s => s
    }
    Some(Function(template.domain, template.range, mit, function.getMetadata))
  }
  
}


object Operation {
    
  /**
   * Construct an Operation subclass based on the given name.
   */
  def apply(opName: String): Operation = apply(opName, Seq[String]())
  //TODO: accept the string form of the expression, without the args separated out

  /**
   * Construct an Operation subclass based on the given name and arguments.
   */
  def apply(opName: String, args: Seq[String]): Operation = this.synchronized {
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
    val moduleSymbol = currentMirror.classSymbol(cls).companion.asModule
    currentMirror.reflectModule(moduleSymbol).instance.asInstanceOf[OperationFactory] 
  }
    
}
