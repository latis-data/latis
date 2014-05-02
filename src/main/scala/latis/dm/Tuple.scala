package latis.dm

import latis.data.Data
import latis.data.EmptyData
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata

import scala.collection.Seq
import scala.collection.immutable

/**
 * Base type for a Variable that represents a group of other Variables.
 */
trait Tuple extends Variable {
  def getVariables: Seq[Variable]
}

  
object Tuple {
  
  def apply(vars: immutable.Seq[Variable], md: Metadata, data: Data): AbstractTuple = new AbstractTuple(vars, md, data)
  
  def apply(vars: Seq[Variable], md: Metadata): AbstractTuple = new AbstractTuple(vars.toIndexedSeq, md)
  
  def apply(v: Variable, md: Metadata): AbstractTuple = new AbstractTuple(List(v), md)
  
  def apply(vars: Seq[Variable]): AbstractTuple = new AbstractTuple(vars.toIndexedSeq)
  
  def apply(v: Variable, vars: Variable*): Tuple = Tuple(v +: vars) 

  /**
   * Expose the Variables contained within this Tuple.
   */
  def unapply(tup: Tuple): Option[Seq[Variable]] = Some(tup.getVariables)
}