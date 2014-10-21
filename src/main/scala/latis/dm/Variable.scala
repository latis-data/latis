package latis.dm

import latis.data.Data
import latis.data.NumberData
import latis.metadata.Metadata
import scala.collection.Seq
import latis.time.Time

/**
 * Base type for all Variables in the LaTiS data model.
 */
trait Variable {
  def getMetadata(): Metadata //need () to avoid ambiguity
  def getMetadata(name: String): Option[String] = getMetadata.get(name)
  def getData: Data
  
  def getName: String
  
  def isNumeric: Boolean = getData.isInstanceOf[NumberData]
  def getNumberData: NumberData = getData.asInstanceOf[NumberData]
  //TODO: deal with TextData
  
//  def getLength: Int
  def getSize: Int
  
  def findVariableByName(name: String): Option[Variable]
  def hasName(name: String): Boolean
  
  def toSeq: Seq[Scalar]
  
  //Experimental: to help build a Scalar from an existing Scalar but with new metadata.
  //should also match tsml element names
  def getType: String = this match {
    case _: Dataset  => "dataset"
    case _: Tuple    => "tuple"
    case _: Function => "function"
    //case _: Time     => "time"  //TODO: how to deal with real vs int...?
    case _: Index    => "index"
    case _: Real     => "real"
    case _: Integer  => "integer"
    case _: Text     => "text"
    case _: Binary   => "binary"
  }
  
  //experimental: build from template with data
  //TODO: consider scala's CanBuildFrom...
  //TODO: pattern match here or do in subclasses?
  def apply(data: Data): Variable = this match {
    //TODO: make sure Data is valid, length
    case t: Time => {
      val scale = t.getUnits
      val md = t.getMetadata
      t match {
        case _: Text => new Time(scale, md, data) with Text
        case _: Real => new Time(scale, md, data) with Real
        case _: Integer => new Time(scale, md, data) with Integer
      }
    }
  }
}

