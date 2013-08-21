package latis.dm

import scala.collection.immutable._
import latis.data.EmptyData
import latis.data.Data
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.ops.math.BasicMath
import latis.ops._

class Dataset(vars: Seq[Variable]) extends Tuple(vars) with BasicMath {
//class Dataset(vars: Seq[Variable], operations: Seq[Operation]) extends Tuple(vars) with BasicMath {
  
  //convenient method, get number of samples in top level Function
  def length: Int = vars.find(_.isInstanceOf[Function]) match {
    case Some(f: Function) => f.length
    case _ => if (vars.isEmpty) 0 else 1
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
  
  //TODO: apply operations, or just do in adapter?
  //operations.foldRight(this)(_(_)) //operation.apply(ds)
  
  //convenience methods for transforming Dataset
  def filter(selection: Selection): Dataset = selection(this)
  def filter(expression: String): Dataset = Selection(expression)(this)
  
  def project(proj: Projection): Dataset = proj(this)
  def project(varNames: Seq[String]): Dataset = Projection(varNames)(this)
  def project(vname: String): Dataset = Projection(Seq(vname))(this)
  
  //TODO: map? operate?
  
  
  //def foreach(f: (Variable,Variable) => (Variable,Variable)): Unit
  //TODO: need to impl "filter" so we can use generator: "for((d,r) <- ds)"
  
  /**
   * Expose the top level Variables in this Dataset as a Single Variable.
   * If it contains a single Variable, return it.
   * If multiple Variables, return them packaged in a Tuple.
   */
  def unwrap: Variable = {
    vars.length match {
      case 1 => vars(0)
      case _ => Tuple(vars, metadata, data)  //TODO: could we just return this since Dataset ISA Tuple? pattern matching problems?
    }
  }
}

object Dataset {
  import scala.collection._
  
  def apply(vars: Seq[Variable]): Dataset = new Dataset(vars.toIndexedSeq)
  
  def apply(vars: Seq[Variable], md: Metadata): Dataset = {
    val ds = new Dataset(vars.toIndexedSeq)
    ds._metadata = md
    ds
  }
  
//  def apply(vars: Seq[Variable], ops: Seq[Operation], md: Metadata): Dataset = {
//    val ds = new Dataset(vars.toIndexedSeq, ops.toIndexedSeq)
//    ds._metadata = md
//    ds
//  }
  
  def apply(v: Variable, vars: Variable*): Dataset = apply(v +: vars)
  
}