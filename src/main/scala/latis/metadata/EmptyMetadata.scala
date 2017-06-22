package latis.metadata

object EmptyMetadata extends Metadata {
  def get(key: String): Option[String] = None
  def getOrElse(key: String, default: => String): String = default
  def getProperties: Map[String,String] = Map[String,String]() 
  def has(key: String): Boolean = false
  override def isEmpty: Boolean = true
  override def nonEmpty: Boolean = false
}
