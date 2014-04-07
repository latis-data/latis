package latis.dm

import scala.collection._
import latis.data.EmptyData
import latis.data.Data
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.ops.math.BasicMath
import latis.ops._
import latis.util.DataMap

class Dataset(variables: immutable.Seq[Variable], metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractTuple(variables, metadata, data) with BasicMath {
  
  //TODO: evaluate for given domain value
  //TODO: Variable already has apply(int)
//  def apply(v: Any): Dataset = v match {
//    case index: Int => ??? //get index-th sample of function, or element of tuple? what about F of Tuple? probably better to use diff method
//    case p: Product => ??? //multi-dim domain
//    case _ => ??? //TODO: make Scalar and eval Function?
//    //error if ds does not have single Function? or recurse till we find a function with a consistent domain?
//  }
  
/*
 * TODO: 
 * consider map(f: Variable => Variable)
 * encapsulate the usual pattern match
 * 
 * flatMap(f: Variable => Dataset) ?
 */
//  def map(f: Variable => Variable): Dataset = {
//    Dataset(variables.map(v => v match {
//      case _: Scalar => f(v)
//      case Tuple(vars) => vars.map(f(_))
//      case fun: Function => Function(fun.iterator.map(f(_)))
//    }))
//  }
  
  //convenient method, get number of samples in top level Function
  //TODO: what if we have multiple Functions...?
  //TODO: better name: getSampleCount?
  def length: Int = findFunction match {
    case Some(f: Function) => f.getLength
    case _ => if (variables.isEmpty) 0 else 1  //TODO: delegate to super? but want "1" as in only one sample, more indications that "length" is too overloaded
  }
  
  def findFunction: Option[Function] = findFunction(this)
  
  //TODO: put in Variable?
  private def findFunction(variable: Variable): Option[Function] = variable match {
    case _: Scalar => None
    case Tuple(vars) => {
      val fs = vars.flatMap(findFunction(_))
      if (fs.nonEmpty) Some(fs.head) else None
    }
    case f: Function => Some(f)
  }
  
  /*
   * TODO: 2013-07-16 Do we need operations?
   * it seems that this was an attempt to lazily apply them
   * could we apply them earlier (e.g. TsmlAdapter)?
   * A Wrapped Function could provide laziness
   * What about non tsml use cases?
   *   make it part of the Reader?
   *   getDataset(ops)
   *   otherwise, directly apply as needed
   */
  
  /*
   * TODO: 2013-06-11
   * careful: is Dataset a collection of Variables or a collection of samples in our monadic context?
   * do we need multiple monads:
   *   Dataset: metadata and data? (currently any Variable)
   *   Tuple: member Variables
   *   Function: samples (but may be continuous)
   *   Scalar: single datum?
   * what should we get from Dataset.iterator?
   *   should we require that a Dataset have only a single top level Var = Function?
   *   or just iterate over member Vars like any other Tuple?
   * consider Variables as scala collections
   *   Function IS-A SortedMap (at least for sampled functions) and has its notion of iteration
   *   Tuple contains Vars thus should iterate over Vars, Dataset likewise?
   *   it's the recursion via pattern matching that invokes the appropriate behavior
   * 
   *   
   * DSL user should always operate at the Dataset level, maintain provenance...
   * morphisms and writers need unfettered access to stuff
   * ++avoid wrapping Function samples as Scalars?
   * save for next version, with better use of monads, flatMap,...
   * 
   * Should Dataset be a separate class from Tuple?
   * not a Variable but the Monad that encapsulates Variables, and Morphisms?
   * Do we ever need Dataset to behave like a Tuple?
   * Can we safely add a morphism to a Dataset instead of making a MorphedFunction?
   * but if Function is a different kind of monad (samples) we may want to attach the morphism there
   * 
   * What about DatasetWrapper as decorator?
   * maybe a Dataset can contain a Dataset? 
   *   consider aggregation: a Dataset encapsulating multiple Datasets with instructions to combine them into a single Dataset
   * ++compare to scala's FilterMonadic, collections have inner class WithFilter
   * 
   */
  
  //convenience methods for transforming Dataset
  //TODO: operate?
  def filter(selection: Selection): Dataset = selection(this)
  def filter(expression: String): Dataset = Selection(expression)(this)
  
  def project(proj: Projection): Dataset = proj(this)
  def project(varNames: Seq[String]): Dataset = Projection(varNames)(this)
  def project(vname: String): Dataset = Projection(Seq(vname))(this)
  
  //Convenient data dumping methods.
  def toDoubleMap = DataMap.toDoubleMap(this)
  def toDoubles   = DataMap.toDoubles(this)
  def toStringMap = DataMap.toStringMap(this)
  def toStrings   = DataMap.toStrings(this)
  
  def groupBy(name: String): Dataset = {
    //TODO: impl as Operation?
    val vs = getVariables.map(_.groupVariableBy(name))
    Dataset(vs) //TODO: metadata
  }
  
  //TODO: operate?
  
  
  //def foreach(f: (Variable,Variable) => (Variable,Variable)): Unit
  //TODO: need to impl "filter" so we can use generator: "for((d,r) <- ds)"
  
  /**
   * Expose the top level Variables in this Dataset as a Single Variable.
   * If it contains a single Variable, return it.
   * If multiple Variables, return them packaged in a Tuple.
   */
  def unwrap: Variable = {
    variables.length match {
      case 1 => variables.head //only one, drop the Tuple wrapper
      case _ => Tuple(variables, metadata, data) //plain Tuple, TODO: metadata
    }
  }
}

object Dataset {
  
  def apply(vars: Seq[Variable]): Dataset = new Dataset(vars.toIndexedSeq)
  
  //TODO: should metadata be first? e.g. variable name
  def apply(vars: Seq[Variable], md: Metadata): Dataset = new Dataset(vars.toIndexedSeq, metadata = md)
  def apply(v: Variable, md: Metadata): Dataset = new Dataset(List(v), metadata = md)
  
//  def apply(vars: Seq[Variable], ops: Seq[Operation], md: Metadata): Dataset = {
//    val ds = new Dataset(vars.toIndexedSeq, ops.toIndexedSeq)
//    ds._metadata = md
//    ds
//  }
  
  def apply(v: Variable, vars: Variable*): Dataset = Dataset(v +: vars)
  
}