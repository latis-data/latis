package latis.dm

import scala.collection._
import latis.data.EmptyData
import latis.data.Data
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata

//class Tuple(val variables: Seq[Variable], metadata: Metadata, data: Data) extends Variable(metadata, data) {
class Tuple(val variables: immutable.Seq[Variable]) extends Variable {

  def apply(index: Int): Variable = variables(index)
  
}

object Tuple {
  
  def apply(vars: Seq[Variable], md: Metadata, data: Data): Tuple = {
    val t = new Tuple(vars.toIndexedSeq)
    t._metadata = md
    t._data = data
    t
  }
  
  def apply(vars: Seq[Variable]): Tuple = new Tuple(vars.toIndexedSeq)
  def apply(v: Variable, vars: Variable*): Tuple = Tuple(v +: vars) 
  
  def apply(vals: Seq[Seq[Double]])(implicit ignore: Double): Tuple = Tuple(vals.map(Real(_))) //resolve type erasure ambiguity with implicit
  //def apply(vals: Seq[Double]*): Tuple = Tuple(vals.map(Real(_)))  //same after erasure
  def apply(ds: Seq[Double], vals: Seq[Double]*): Tuple = {
    val t = Tuple(Real(ds) +: vals.map(Real(_)))
    //TODO: put data in Tuple?
    t
  }
  
  def unapply(tup: Tuple): Option[Seq[Variable]] = Some(tup.variables)
}