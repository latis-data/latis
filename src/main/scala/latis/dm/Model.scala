package latis.dm

import latis.metadata.Metadata
import scala.collection.TraversableLike
import scala.collection.mutable.Builder
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Stack
import scala.collection.mutable.ArrayBuffer

/*
 * New representation of information in a tsml file.
 * Created after a first pass in an adapter.
 */


sealed abstract class VariableType(id: String, metadata: Metadata) {

  /**
   * Does this Variable have the given name or alias.
   */
  def hasName(name: String): Boolean = {
    //TODO: consider long, qualified name
    //TODO: support wildcard, regex?
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

  //def getMetadata: Metadata = metadata
  //TODO: ambiguous with getMetadata()(name)
  def getMetadata(name: String): Option[String] = metadata.get(name)
  
  def getType: String = metadata.getOrElse("type", "undefined") //TODO: require type?
  
  def getId: String = id

}

case class ScalarType(
    id: String, 
    metadata: Metadata = Metadata.empty
) extends VariableType(id, metadata) {
  
  override def toString: String = id
}
//TODO: put type in metadata? useful for readers of json...
//  type could be a known type or a class name that extends Scalar
//TODO: allow data to be defined?
//TODO: make sure metadata has name, default to id
//  but can't do here with immutable metadata, use smart constructor?

case class TupleType(
    variables: Seq[VariableType],
    id: String = "",
    metadata: Metadata = Metadata.empty
) extends VariableType(id, metadata) {
  
  override def toString: String = variables.mkString("$id: (", ", ", ")")
}
//TODO: allow anonymous tuple? no name in metadata, 
//    but is id still required? "" or auto?


case class FunctionType(
    domain: VariableType,
    codomain: VariableType,
    id: String = "", 
    metadata: Metadata = Metadata.empty
) extends VariableType(id, metadata) {
  
  override def toString: String = s"$domain -> $codomain"
}


case class ProcessingInstruction(
    name: String,
    args: String,
    targetVariable: String = "" //apply to dataset if ""
)


case class Model(
    val variable: VariableType,  //top level variable
    val metadata: Metadata = Metadata.empty,
    pis: Seq[ProcessingInstruction] = Seq.empty
) extends Traversable[VariableType] with TraversableLike[VariableType, Model] {
  //TODO: build Model from traversable method results (e.g. filter returns Traversable)
  
  import Model._
  override protected[this] def newBuilder: Builder[VariableType, Model] = Model.newBuilder
    
  /**
   * Implement Traversable so we can use filter....
   */
  def foreach[U](f: VariableType => U): Unit = {
    def go(v: VariableType): Unit = {
      //apply function
      f(v)
      //recurse
      v match {
        case _: ScalarType     => //end of this branch
        case TupleType(vars,_,_)   => vars.map(go(_))
        case FunctionType(d,c,_,_) => go(d); go(c)
      }
    }
    go(variable)
  }
  
  /**
   * Find the first VariableType (not just ScalarTypes) with the given name.
   */
  def findVariableByName(vname: String): Option[VariableType] = {
    //TODO: support "." in vname, see AbstractVariable
    find(_.hasName(vname))
  }
  
  def findVariableAttribute(vname: String, attribute: String): Option[String] = {
    findVariableByName(vname).flatMap(_.getMetadata(attribute))
  }
  
  def getScalars: Seq[ScalarType] = {
    toSeq.collect { case s: ScalarType => s }
  }
  
  def getDomainVars: Seq[VariableType] = {
    toSeq.collect { case f: FunctionType => f }.map(_.domain)
  }
  
  override def toString: String = s"Model($variable)"
}
//TODO: dataset joins, e.g. defined in tsml
//TODO: construct from model syntax: X -> (A, B)
//TODO: name registry, guarantee unique names? 
//      generate scalar1, scalar2...? uuid?

object Model {
  
/*
 * TODO: Use implicit CanBildFrom to get a Model back from Traversable methods
 * assumes depth first traversal
 */
  def fromSeq(vts: Seq[VariableType]): Model = {
    def go(vts: Stack[VariableType]): VariableType = {
      if (vts.nonEmpty) vts.pop match {
        case s: ScalarType => s
        case TupleType(vars, id, md) => TupleType(vars.map(i => go(vts)), id, md)
        case FunctionType(_,_,id,md) => FunctionType(go(vts), go(vts), id, md)
      } else ??? //Error: ran out of variables
    }
    
    Model(go(Stack() ++ vts))
  }
  
  def newBuilder: Builder[VariableType, Model] = new ArrayBuffer mapResult fromSeq

  implicit def canBuildFrom: CanBuildFrom[Model, VariableType, Model] =
    new CanBuildFrom[Model, VariableType, Model] {
      def apply(): Builder[VariableType, Model] = newBuilder
      def apply(from: Model): Builder[VariableType, Model] = newBuilder
    }

}
