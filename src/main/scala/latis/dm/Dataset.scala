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
 * The main abstraction for a dataset that encapsulates everything about the dataset. 
 * A Dataset should (must?) contain a single top level Variable and optionally Metadata.
 */
class Dataset(variable: Variable, metadata: Metadata = EmptyMetadata) extends BasicMath {
  //TODO: should we have a special class of DatasetMetadata? akin to NetCDF global attributes
  
  val getMetadata = metadata
  
  //TODO: consider if dataset must have name, generate a unique identifier?
  def getName = metadata.getOrElse("name", "")
  
  def isEmpty = variable == null
  
  //convenient method, get number of samples in top level Function
  //TODO: what if we have multiple Functions...?
  //TODO: better name: getSampleCount?
  //TODO: should we account for length of nested Functions?
  def getLength = variable match {
    case f: Function => f.getLength
    case _ if (variable != null) => 1
    case _ => 0
  }
  
  def findVariableByName(name: String): Option[Variable] = variable match {
    case null => None
    case _ => variable.findVariableByName(name)
  }
  
  //convenience methods for transforming Dataset
  def filter(selection: Selection): Dataset = selection(this)
  def filter(expression: String): Dataset = Selection(expression)(this)
  
  def project(proj: Projection): Dataset = proj(this)
  def project(varNames: Seq[String]): Dataset = Projection(varNames)(this)
  def project(vname: String): Dataset = Projection(Seq(vname))(this)
  
  def select(expression: String): Dataset = Selection(expression)(this)
  
  def rename(origName: String, newName: String): Dataset = RenameOperation(origName, newName)(this)
  
  def replaceValue(v1: Any, v2: Any): Dataset = ReplaceValueOperation(v1,v2)(this)
  
  def reduce = Reduction.reduce(this)
  def flatten = reduce
  
  def intersect(that: Dataset): Dataset = Intersection(this, that)
  
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
   * Until we can enforce sorting of function samples this will do so. 
   * Assumes Function with Integer domain only, for now.
   * Sort by range if domain is Index.
   * TODO: implement as Operation.
   * See latis-mms-web TestDatasets
   */
  def sorted: Dataset = variable match {
    case f @ Function(samples) => {
      f.getDomain match {
        case _: Index   => Dataset(Function(samples.toSeq.sortBy(s => s match {case Sample(_, Tuple(Seq(d: Integer))) => d})(Integer(0))))
        case _: Integer => Dataset(Function(samples.toSeq.sortBy(s => s match {case Sample(d: Integer, _) => d})(Integer(0))))
      }
    }
  }
  
  def last: Dataset = variable match {
    //TODO: use Last filter
    case Function(samples) => {
      Dataset(Function(List(samples.toSeq.last)))
    }
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
  //See how far we can get with pattern matching: Dataset(v)
  
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
  
  
  //extract the contained Variable
  def unapply(dataset: Dataset): Option[Variable] = {
    val v = dataset.unwrap
    if (v == null) None
    else Some(v)
  }
  
}