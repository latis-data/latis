package latis.metadata


/**
 * Just name/value pairs, for now.
 */
trait Metadata {
  def getProperties: Map[String,String]
  
  def get(key: String): Option[String]
  
  def apply(key: String): String = get(key) match {
    case Some(s) => s
    case None => null //TODO: error? "unknown"?
  }
  
  def has(key: String): Boolean
  
  lazy val name = get("name") match {
    case Some(s) => s
    case None => "unknown" //TODO: do we want default name to be "unknown"? generate a unique id?
  }
  
  override def toString() = name
}


object Metadata {
  
  import scala.collection._
  
  val empty = EmptyMetadata
  
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
