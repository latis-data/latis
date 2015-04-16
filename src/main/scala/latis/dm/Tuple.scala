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
  
  /**
   * Return the Variables directly contained in this Tuple as a Seq.
   */
  def getVariables: Seq[Variable]
  
  /**
   * Return the number of elements in this Tuple.
   */
  def getArity: Int = getVariables.length
  def getElementCount: Int = getVariables.length //more intuitive?
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

/**
 * Utility object for use with pattern matching. You can pull
 * individual elements out of a Tuple like this:
 * 
 * x match {
 *   TupleMatch(t: Text, f: Function) => ???
 *   _ => ???
 * }
 * 
 * Note: You will have to "import latis.dm.TupleMatch" to be
 * able to see this.
 */
object TupleMatch {
  /*
   * This TupleMatch.unapplySeq has exactly the same definition and
   * return type as Tuple.unapply. The language lets you
   * do different things with unapplySeq vs unapply, but gets
   * confused when they use the same return types, so I had to
   * pull this one out.
   */
  def unapplySeq(tup: Tuple): Option[Seq[Variable]] = Some(tup.getVariables)
}