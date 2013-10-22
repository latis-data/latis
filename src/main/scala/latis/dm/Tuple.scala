package latis.dm

import scala.collection._
import latis.data.EmptyData
import latis.data.Data
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata

trait Tuple extends Variable { //this: Variable =>
  def getVariables: immutable.Seq[Variable]
  
  //TODO: move to Variable
  def apply(index: Int): Variable = getVariables(index)
  
}

class TupleVariable(variables: immutable.Seq[Variable], metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends Variable2(metadata, data) with Tuple {
  def getVariables = variables
}

object Tuple {
  
  def apply(vars: immutable.Seq[Variable], md: Metadata, data: Data): TupleVariable = new TupleVariable(vars, md, data)
  
  def apply(vars: Seq[Variable]): TupleVariable = new TupleVariable(vars.toIndexedSeq)
//  def apply(v: Variable, vars: Variable*): Tuple = Tuple(v +: vars) 
//  
//  def apply(vals: Seq[Seq[Double]])(implicit ignore: Double): Tuple = Tuple(vals.map(Real(_))) //resolve type erasure ambiguity with implicit
//  //TODO: apply(vals: Seq[_]) = vals(0) match ...
//  //def apply(vals: Seq[Double]*): Tuple = Tuple(vals.map(Real(_)))  //same after erasure
//  def apply(ds: Seq[Double], vals: Seq[Double]*): Tuple = {
//    val t = Tuple(Real(ds) +: vals.map(Real(_)))
//    //TODO: put data in Tuple?
//    t
//  }
  
  def unapply(tup: Tuple): Option[Seq[Variable]] = Some(tup.getVariables)
}