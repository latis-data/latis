package latis.dm

import scala.collection.immutable._

/**
 * A Variable that represents a collection of Variables.
 */
class Tuple(val variables: Seq[Variable]) extends Variable {

  //Set parentage
  variables.map(_.setParent(this))
}

object Tuple {
  
  def apply(vars: Variable*) = new Tuple(vars.toIndexedSeq) //need an immutable Seq
  
  //expose the Variables that the given Tuple contains
  def unapply(tup: Tuple): Option[Seq[Variable]] = Some(tup.variables)
}