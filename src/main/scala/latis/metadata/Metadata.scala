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
  
  /**
   * Add a property returning a new Metadata object.
   * Replace if it already exists.
   */
  def +(kv: (String,String)): Metadata = Metadata(getProperties + kv)
  
  def has(key: String): Boolean
  
  def isEmpty = getProperties.isEmpty
  def nonEmpty = getProperties.nonEmpty
   
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
  
  /**
   * Construct Metadata from a Map of properties.
   */
  def apply(properties: Map[String,String]): Metadata = properties match {
    case null => EmptyMetadata
    case _ => if (properties.isEmpty) EmptyMetadata else new VariableMetadata(properties.toMap)
  }
  
  /**
   * Construct Metadata from a single property.
   */
  def apply(property: (String,String)): Metadata = new VariableMetadata(immutable.Map(property))

}
