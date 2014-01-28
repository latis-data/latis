package latis.dm

import latis.data._
import latis.metadata._
import latis.ops.math.BasicMath
import latis.time.Time
import scala.collection._
import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.ArrayBuffer

/*
 * TODO: 2013-10-21
 * traits: Scalar, Real, Integer,..., Tuple, Function
 * each could have "this: Variable =>"  but not IS-A Variable
 * when to we need to pattern match on Variable vs type?
 * do we need a Variable trait? yes, write to interfaces
 * and a VariableImpl
 */

trait Variable {
  def getMetadata: Metadata
  def getData: Data
  
  def getName: String
  
  def isNumeric: Boolean = getData.isInstanceOf[NumberData]
  def getNumberData: NumberData = getData.asInstanceOf[NumberData]
  //TODO: deal with TextData
  
  def getLength: Int
  def getSize: Int
  
  def getVariableByName(name: String): Option[Variable]
  def hasName(name: String): Boolean
  
  def toSeq: Seq[Scalar]
  def getDataIterator: Iterator[Data]
  def getDomainDataIterator: Iterator[Data]
  
  def getVariables: Seq[Variable] //TODO: immutable.Seq?
  def apply(index: Int): Variable = getVariables(index)
  
  def groupVariableBy(name: String): Function
  //TODO: allow multiple vars for nD
  
  //def reduce: Variable
  
  //TODO: toStringValue?
}


object Variable {
  //TODO: factory methods, mixin math,...
  
  //Use Variable(md, data) in pattern match to expose metadata and data.
  //Subclasses may want to expose data values
  def unapply(v: Variable) = Some((v.getMetadata, v.getData))
}
