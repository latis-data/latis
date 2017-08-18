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


case class Scalar3(
    id: String, 
    metadata: Metadata = Metadata.empty, 
    get: () => Data = () => Data.empty)
  extends Variable3(id, metadata) {
  
//  /**
//   * Return the size in bytes needed to store one data value for this Scalar.
//   */
//  def getSize: Int = getType match {
//    case "integer" => 8  //Long
//    case "real"    => 8  //Double
//    case "text"    => getMetadata("length") match {
//      // 2 bytes per character
//      case Some(n) => n.toInt * 2
//      case None    => Text.DEFAULT_LENGTH * 2
//    }
//  }
  
  override def toString: String = id
}

/*
 * varargs too nuch trouble
 * Pattern match with varargs to get Seq[Variable]: Tuple(_,_, vs @ _*)
 * Construct with Seq: Tuple(id, md, vs: _*)
 * Apparently lost ability to copy.
 */
//case class Tuple3(id: String, metadata: Metadata, variables: Variable3*)
case class Tuple3(
    id: String = "", 
    metadata: Metadata = Metadata.empty, 
    variables: Seq[Variable3])
  extends Variable3(id, metadata) {
  
  lazy val arity = variables.length
  
  override def toString: String = {
    val name = id match {
      case "" => ""
      case _  => s"$id:"
    }
    variables.mkString(s"$name(", ", ", ")")
  }
}
object Tuple3 {
  def apply(vars: Variable3*): Tuple3 = Tuple3("", Metadata.empty, vars.toSeq)
}

//case class Function3(domain: Variable3, codomain: Variable3)(id: String, metadata: Metadata)
//  only extracts d,c but can't use copy(md = md)...
case class Function3(id: String, metadata: Metadata, domain: Variable3, codomain: Variable3)
  extends Variable3(id, metadata) {
  
  override def toString: String = s"$domain -> $codomain"
}

trait SampledFunction3 { this: Function3 =>
  def iterator: Iterator[(Variable3, Variable3)]
}
object SampledFunction3 {
//  def apply(id: String, metadata: Metadata, domain: Variable3, codomain: Variable3): SampledFunction3 =
//    new Function3(id, metadata, domain, codomain) with SampledFunction3 {
//      def iterator: Iterator[(Variable3, Variable3)] = ???
//    }    
  def unapply(f: SampledFunction3) = Option(f.iterator)
}
