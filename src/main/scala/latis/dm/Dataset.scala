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

/**
 * The main container for a dataset. It is a special type of Tuple
 * that encapsulates everything about the dataset. Most operations
 * are performed on Datasets and return new Datasets.
 */
class Dataset(variables: immutable.Seq[Variable], metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractTuple(variables, metadata, data) with BasicMath {
  
  //convenient method, get number of samples in top level Function
  //TODO: what if we have multiple Functions...?
  //TODO: better name: getSampleCount?
  def getLength = findFunction match {
    case Some(f: Function) => f.getLength
    case None => ??? 
  }
  
  /**
   * Return the first top level Function in this Dataset.
   */
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
  
  
  //convenience methods for transforming Dataset
  def filter(selection: Selection): Dataset = selection(this)
  def filter(expression: String): Dataset = Selection(expression)(this)
  
  def project(proj: Projection): Dataset = proj(this)
  def project(varNames: Seq[String]): Dataset = Projection(varNames)(this)
  def project(vname: String): Dataset = Projection(Seq(vname))(this)
  
  def rename(origName: String, newName: String): Dataset = RenameOperation(origName, newName)(this)
  
  def replaceValue(v1: ScalaNumericAnyConversions, v2: ScalaNumericAnyConversions): Dataset = ReplaceValueOperation(v1,v2)(this)
  
  
  //Convenient data dumping methods.
  def toDoubleMap = DataMap.toDoubleMap(this)
  def toDoubles   = DataMap.toDoubles(this)
  def toStringMap = DataMap.toStringMap(this)
  def toStrings   = DataMap.toStrings(this)
  
  def groupBy(name: String): Dataset = {
    val vs = variables.map(v => Factorization.groupVariableBy(v, name))
    Dataset(vs) //TODO: metadata
  }
  
  
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
  
  def apply(vars: Seq[Variable], md: Metadata): Dataset = new Dataset(vars.toIndexedSeq, metadata = md)
  def apply(v: Variable, md: Metadata): Dataset = new Dataset(List(v), metadata = md)
  
  def apply(v: Variable, vars: Variable*): Dataset = Dataset(v +: vars)
  
}