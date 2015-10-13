package latis.dm

import latis.data.Data
import latis.metadata.Metadata

/**
 * Naught is a placeholder that essentially represents
 * "No variable here". It is mostly only useful for
 * constructing empty functions that have neither
 * domain nor range (i.e. they're a function of
 * Naught -> Naught)
 */
class Naught extends Variable {
  
  def findFunction: Option[latis.dm.Function] = None
  
  def findVariableByName(name: String): Option[latis.dm.Variable] = None
  def findAllVariablesByName(name: String) = Seq()
  
  def getData: Data = Data.empty
  
  def getMetadata(): Metadata = Metadata.empty
  
  def hasName(name: String): Boolean = false
  def getName: String = ""
  
  def getSize: Int = 0
  
  def toSeq: Seq[Scalar] = Seq.empty
}

object Naught {
  def apply(): Naught = new Naught()
}