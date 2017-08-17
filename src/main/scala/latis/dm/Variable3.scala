package latis.dm

import latis.metadata.Metadata
import latis.data.Data

sealed abstract class Variable3(id: String, metadata: Metadata) {
  /**
   * Does this Variable have the given name or alias.
   */
  def hasName(name: String): Boolean = {
    id == name || metadata.get("name").exists(_ == name) || hasAlias(name)
  }

  /**
   * Does this Variable have an alias that matches the given name.
   */
  def hasAlias(alias: String): Boolean = {
    metadata.get("alias") match {
      case Some(s) => s.split(",").contains(alias)
      case None => false
    }
  }

  def getMetadata(name: String): Option[String] = metadata.get(name)
  
  def getType: String = metadata.getOrElse("type", "undefined") //TODO: require type?
  
  def getId: String = id
}


case class Scalar3(id: String, metadata: Metadata, get: () => Data)
  extends Variable3(id, metadata) {
  
  override def toString: String = id
}


case class Tuple3(id: String, metadata: Metadata, variables: Variable3*)
  extends Variable3(id, metadata) {
  
  override def toString: String = {
    val name = id match {
      case "" => ""
      case _  => s"$id:"
    }
    variables.mkString(s"$name(", ", ", ")")
  }
}


//case class Function3(domain: Variable3, codomain: Variable3)(id: String, metadata: Metadata)
//  only extracts d,c but can't use copy(md = md)...
case class Function3(id: String, metadata: Metadata, domain: Variable3, codomain: Variable3)
  extends Variable3(id, metadata) {
  
  override def toString: String = s"$domain -> $codomain"
}

trait SampledFunction3 { //TODO: self type of Function?
  def iterator: Iterator[(Variable3, Variable3)]
}
object SampledFunction3 {
  def unapply(f: SampledFunction3) = Option(f.iterator)
}
