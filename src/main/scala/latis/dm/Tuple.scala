package latis.dm

import scala.collection._
import latis.data.EmptyData
import latis.data.Data
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata

trait Tuple extends Variable { //this: Variable =>
  
}

class AbstractTuple(variables: immutable.Seq[Variable], metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractVariable(metadata, data) with Tuple {
  override def getVariables = variables
}

object Tuple {
  
  def apply(vars: immutable.Seq[Variable], md: Metadata, data: Data): AbstractTuple = new AbstractTuple(vars, md, data)
  
  def apply(vars: Seq[Variable], md: Metadata): AbstractTuple = new AbstractTuple(vars.toIndexedSeq, md)
  
  def apply(v: Variable, md: Metadata): AbstractTuple = new AbstractTuple(List(v), md)
  
  def apply(vars: Seq[Variable]): AbstractTuple = new AbstractTuple(vars.toIndexedSeq)
  
  def apply(v: Variable, vars: Variable*): Tuple = Tuple(v +: vars) 

//  def apply(vals: Seq[Seq[Double]])(implicit ignore: Double): Tuple = {  //resolve type erasure ambiguity with implicit
//    //for ((v,i) <- vals.zipWithIndex)
//    val reals = vals.zipWithIndex.map(p => Real(Metadata("real"+p._2), p._1))
//    Tuple(reals)
//  }

  //  //TODO: apply(vals: Seq[_]) = vals(0) match ...
//  //def apply(vals: Seq[Double]*): Tuple = Tuple(vals.map(Real(_)))  //same after erasure
//  def apply(ds: Seq[Double], vals: Seq[Double]*): Tuple = {
//    val t = Tuple(Real(ds) +: vals.map(Real(_)))
//    //TODO: put data in Tuple?
//    t
//  }
  
  def unapply(tup: Tuple): Option[Seq[Variable]] = Some(tup.getVariables)
}