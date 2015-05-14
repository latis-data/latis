package latis.metadata

import scala.collection.immutable.Map

class VariableMetadata(val properties: Map[String,String]) extends Metadata {
  
  def getProperties = properties
  
  def get(key: String) = properties.get(key)
  
  def getOrElse(key: String, default: => String) = properties.getOrElse(key, default)
  
  def has(key: String): Boolean = properties.contains(key)
  
  override def equals(that: Any): Boolean = that match {
    case md: VariableMetadata => md.properties == properties
    case _ => false
  }
  
  override def hashCode = properties.hashCode
}
