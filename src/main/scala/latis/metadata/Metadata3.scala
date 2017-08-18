package latis.metadata

import scala.collection.TraversableLike
import scala.collection.mutable.Builder
import scala.collection.mutable.Stack
import scala.collection.mutable.ArrayBuffer
import scala.collection.generic.CanBuildFrom

case class Metadata3(metadata: VariableMetadata3)(val properties: Map[String, String])
  extends Traversable[VariableMetadata3] with TraversableLike[VariableMetadata3, Metadata3] {
   
  override protected[this] def newBuilder: Builder[VariableMetadata3, Metadata3] = Metadata3.newBuilder
    
  /**
   * Implement Traversable so we can use filter....
   */
  def foreach[U](f: VariableMetadata3 => U): Unit = {
    def go(v: VariableMetadata3): Unit = {
      //recurse
      v match {
        case _: ScalarMetadata       => //end of this branch
        case TupleMetadata(vs)    => vs.map(go(_))
        case FunctionMetadata(d,c) => go(d); go(c)
      }
      //apply function after taking care of kids = depth first
      f(v)
    }
    go(metadata)
  }
  
  // Convenient property methods
  def get(key: String): Option[String] = properties.get(key)
  def getOrElse(key: String, default: => String): String = 
    properties.getOrElse(key, default)
    
  def findVariableByName(name: String): Option[VariableMetadata3] =
    find(_.hasName(name))
    
  def findVariableProperty(name: String, key: String): Option[String] =
    findVariableByName(name).flatMap(_.get(key))
}

//=============================================================================

sealed abstract class VariableMetadata3(properties: Map[String, String]) {
  // Convenient property methods
  def get(key: String): Option[String] = properties.get(key)
  def getOrElse(key: String, default: => String): String = 
    properties.getOrElse(key, default)
    
  /**
   * Does this Variable have the given name or alias.
   */
  def hasName(name: String): Boolean = {
    get("id").exists(_ == name) || get("name").exists(_ == name) || hasAlias(name)
  }

  /**
   * Does this Variable have an alias that matches the given name.
   */
  def hasAlias(alias: String): Boolean = get("alias") match {
    case Some(s) => s.split(",").contains(alias)
    case None => false
  }
}
  
case class ScalarMetadata(properties: Map[String, String]) 
  extends VariableMetadata3(properties)

case class TupleMetadata(members: Seq[VariableMetadata3])
  (val properties: Map[String, String])
  extends VariableMetadata3(properties)

case class FunctionMetadata(domain: VariableMetadata3, codomain: VariableMetadata3)
  (val properties: Map[String, String])
  extends VariableMetadata3(properties)

//=============================================================================

object Metadata3 {
  
  val empty = Metadata3(null)(Map.empty)
  
  def fromSeq(vars: Seq[VariableMetadata3]): Metadata3 = {
    def go(vs: Seq[VariableMetadata3], hold: Stack[VariableMetadata3]): VariableMetadata3 = {
      vs.headOption match {
        case Some(s: ScalarMetadata) => 
          go(vs.tail, hold.push(s)) //put on the stack then do the rest
        case Some(t: TupleMetadata) => 
          val n = t.members.length
          val t2 = TupleMetadata((0 until n).map(_ => hold.pop).reverse)(t.properties)
          go(vs.tail, hold.push(t2))
        case Some(f: FunctionMetadata) => 
          val c = hold.pop
          val d = hold.pop
          val f2 = FunctionMetadata(d, c)(f.properties)
          go(vs.tail, hold.push(f2))
        case None => hold.pop //TODO: test that the stack is empty now
      }
    }
    //Note, no top level properties
    Metadata3(go(vars, Stack()))(Map.empty)
  }
  
  def newBuilder: Builder[VariableMetadata3, Metadata3] = new ArrayBuffer mapResult fromSeq

  implicit def canBuildFrom: CanBuildFrom[Metadata3, VariableMetadata3, Metadata3] =
    new CanBuildFrom[Metadata3, VariableMetadata3, Metadata3] {
      def apply(): Builder[VariableMetadata3, Metadata3] = newBuilder
      def apply(from: Metadata3): Builder[VariableMetadata3, Metadata3] = newBuilder
    }
}
