package latis.metadata

import scala.collection.immutable.Map

class VariableMetadata(val properties: Map[String,String]) extends Metadata {
  def get(key: String) = properties.get(key)
  
  override def equals(that: Any): Boolean = that match {
    case md: VariableMetadata => md.properties == properties
    case _ => false
  }
  
  override def hashCode = properties.hashCode
}
