package latis.dm

import latis.data.Data
import latis.data.NumberData
import latis.metadata.Metadata

import scala.collection.Seq

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
  
  def getLength: Int
  def getSize: Int
  
  def findVariableByName(name: String): Option[Variable]
  def hasName(name: String): Boolean
  
  def toSeq: Seq[Scalar]
  
}

