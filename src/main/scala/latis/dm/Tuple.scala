package latis.dm

import scala.collection.immutable._

/**
 * A Variable that represents a collection of Variables.
 */
class Tuple(val variables: Seq[Variable]) extends Variable {

}

object Tuple {
  
  //expose the Variables that the given Tuple contains
  def unapply(tup: Tuple): Option[Seq[Variable]] = Some(tup.variables)
}