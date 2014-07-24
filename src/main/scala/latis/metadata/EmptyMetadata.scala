package latis.metadata

object EmptyMetadata extends Metadata {
  def get(key: String) = None
  def getProperties: Map[String,String] = Map[String,String]() 
  def has(key: String): Boolean = false
  override def isEmpty = true
  override def nonEmpty = false
}