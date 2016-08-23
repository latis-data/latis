package latis.ops

import latis.dm.Variable

/*
 * Defines an implicit ordering on Variables.
 * This is used by the latis Sort operation.
 */
class VariableOrdering(ordering: Ordering[Variable]) extends Ordering[Variable] {
  def compare(x: Variable, y: Variable): Int = x compare y
}

object VarSort {
  def apply[T](variables: Seq[Variable])(implicit ordering: Ordering[Variable]) = {
    val varOrdering = new VariableOrdering(ordering)
    variables.sorted(varOrdering)
  }
}