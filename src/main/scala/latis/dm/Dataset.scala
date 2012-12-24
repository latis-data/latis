package latis.dm

import latis.reader.DatasetAccessor

import scala.collection.immutable._

/**
 * A Dataset is the ultimate parent of a collection of Variables.
 * Unlike other Variables, a Dataset is responsible for data access.
 * A Variable must get at its data via its parent Dataset.
 */
class Dataset(accessor: DatasetAccessor, vars: Seq[Variable]) extends Tuple(vars) {

}

object Dataset {
  def apply(accessor: DatasetAccessor, vars: Variable*) = new Dataset(accessor, vars.toIndexedSeq)
}