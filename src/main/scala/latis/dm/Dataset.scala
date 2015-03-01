package latis.dm

import latis.data.Data
import latis.data.EmptyData
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.ops.Projection
import latis.ops.filter.Selection
import latis.ops.math.BasicMath
import latis.util.DataMap
import scala.Option.option2Iterable
import scala.collection.Seq
import scala.collection.immutable
import latis.ops.Factorization
import latis.ops.RenameOperation
import latis.ops.ReplaceValueOperation
import scala.math.ScalaNumericAnyConversions
import latis.ops.agg.Intersection
import latis.ops.Reduction
import latis.ops.Memoization

/**
 * The main container for a dataset. It is a special type of Tuple
 * that encapsulates everything about the dataset. Most operations
 * are performed on Datasets and return new Datasets.
 */
//class Dataset(variables: immutable.Seq[Variable], metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
//  extends AbstractTuple(variables, metadata, data) with BasicMath {
class Dataset(variable: Variable, metadata: Metadata = EmptyMetadata) extends BasicMath {
  /*
   * TODO: Dataset that is not a Tuple
   * Mixed concerns.
   * Variable must live in context of a Dataset, monadic
   * Dataset as "special kind of" Variable leads to exceptions to the rule
   * 
   * what about metadata?
   * should it belong to the top variable?
   * global attributes: history,...
   * seems like we need something at the Dataset level
   * nc distinguishes between global and variable attributes
   * duplicate metadata logic?
   * Metadata as mixin?
   * We do have a VariableMetadata class, why not DatasetMetadata?
   * consider how this maps to the dataset ontology
   * 
   * What about metadata modeled as a Variable?
   *   instead of special kind of data
   * 
   * Is there a use case for a Dataset with multiple vars?
   * could always wrap in a Tuple
   * 
   * idiomatic operation on Function
   * no need to iterate over multiple vars, only one
   * 
   * Aggregation currently makes a Dataset containing multiple Datasets
   * should we just make this a Tuple and have aggregate combine the tuple elements?
   * or should agg be a binary+ operation with arg for each dataset?
   * Operations impl apply(dataset):Dataset
   * should agg be something other than operation?
   * construct agg with other datasets?
   * 
   * +consider how best to deal with empty dataset
   * variable is null
   * should we pattern match on Dataset(v) vs empty?
   */
  
  val getMetadata = metadata
  
  //TODO: consider if dataset must have name
  def getName = metadata.getOrElse("name", "")
  
  def isEmpty = variable == null
  
  //convenient method, get number of samples in top level Function
  //TODO: what if we have multiple Functions...?
  //TODO: better name: getSampleCount?
  //TODO: should we account for length of nested Functions?
  def getLength = variable match {
    case f: Function => f.getLength
    case _ => ??? 
  }
  
  /**
   * Return the first top level Function in this Dataset.
   */
  //def findFunction: Option[Function] = findFunction(this)
  
  //TODO: put in Variable?
//  private def findFunction(variable: Variable): Option[Function] = variable match {
//    case _: Scalar => None
//    case Tuple(vars) => {
//      val fs = vars.flatMap(findFunction(_))
//      if (fs.nonEmpty) Some(fs.head) else None
//    }
//    case f: Function => Some(f)
//  }
  
  
  //convenience methods for transforming Dataset
  def filter(selection: Selection): Dataset = selection(this)
  def filter(expression: String): Dataset = Selection(expression)(this)
  
  def project(proj: Projection): Dataset = proj(this)
  def project(varNames: Seq[String]): Dataset = Projection(varNames)(this)
  def project(vname: String): Dataset = Projection(Seq(vname))(this)
  
  def rename(origName: String, newName: String): Dataset = RenameOperation(origName, newName)(this)
  
  def replaceValue(v1: ScalaNumericAnyConversions, v2: ScalaNumericAnyConversions): Dataset = ReplaceValueOperation(v1,v2)(this)
  
  def reduce = Reduction.reduce(this)
  
  def intersect(that: Dataset): Dataset = {
    //tmp hack until we refactor agg
    val dataset = Dataset(Tuple(this.unwrap, that.unwrap))
    Intersection()(dataset)
  }
  
  //Convenient data dumping methods.
  def toDoubleMap = DataMap.toDoubleMap(this)
  def toDoubles   = DataMap.toDoubles(this)
  def toStringMap = DataMap.toStringMap(this)
  def toStrings   = DataMap.toStrings(this)
  
  def groupBy(name: String): Dataset = {
    val v = Factorization.groupVariableBy(variable, name)
    Dataset(v) //TODO: metadata
  }
  
  /**
   * Realize the Data for this Dataset so we can close the Reader.
   * Inspired by Scala's Stream.force.
   * This will return a new Dataset that is logicall equivalent.
   */
  def force: Dataset = Memoization()(this)
  
  /**
   * Expose the top level Variable in this Dataset.
   */
  def unwrap: Variable = variable
  //TODO: better name? getVariable? head? other monadic ways? use unapply? expose variable?
  
  
  override def equals(that: Any): Boolean = that match {
    case thatds: Dataset => (this.getMetadata == thatds.getMetadata) && (this.unwrap == thatds.unwrap)
    case _ => false
  }
  
  override def toString() = {
    val pre = getMetadata.get("name") match {
      case Some(s) => s + ": "
      case None => ""
    }
    pre + "(" + variable.toString + ")"
  }
}


object Dataset {
  
  def apply(v: Variable, md: Metadata): Dataset = new Dataset(v, metadata = md)
  def apply(v: Variable): Dataset = new Dataset(v)
  def apply(): Dataset = new Dataset(null)
  
  val empty = Dataset()
  
  //def apply(vars: Seq[Variable]): Dataset = new Dataset(vars.toIndexedSeq)
  
  //def apply(vars: Seq[Variable], md: Metadata): Dataset = new Dataset(vars.toIndexedSeq, metadata = md)
  
  //def apply(v: Variable, vars: Variable*): Dataset = Dataset(v +: vars)
  
  //extract the contained Variable
  def unapply(dataset: Dataset): Option[Variable] = {
    val v = dataset.unwrap
    if (v == null) None
    else Some(v)
  }
  
}