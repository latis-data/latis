package latis.metadata

/**
 * Just name/value pairs, for now.
 */
trait Metadata {
  def getProperties: Map[String,String]
  
  def get(key: String): Option[String]
  
  def apply(key: String): String = get(key) match {
    case Some(s) => s
    case None => null //TODO: error? default?
  }
  
  def has(key: String): Boolean
   
  override def toString() = getProperties.toString
}


object Metadata {
  
  import scala.collection.immutable
  import scala.collection.Map
  
  val empty = EmptyMetadata
  
  /**
   * Convenience for providing Variables with a name.
   */
  def apply(name: String): Metadata = {
    //TODO: make sure this is a valid name
    val props = immutable.HashMap(("name", name))
    new VariableMetadata(props)
  }
  
  def apply(properties: Map[String,String]): Metadata = properties match {
    case null => EmptyMetadata
    case _ => if (properties.isEmpty) EmptyMetadata else new VariableMetadata(properties.toMap)
  }

}
