package latis.dm

import latis.metadata._
import latis.data.Data
import java.util.UUID

sealed abstract class Variable3(metadata: VariableMetadata3) {

  def getMetadata(name: String): Option[String] = metadata.get(name)
  
  def getType: String = metadata.getOrElse("type", "undefined") //TODO: require type?
  
  lazy val getId: String = getMetadata("id") match {
    case Some(id) => id
    case None => UUID.randomUUID.toString.take(8)
    //TODO: what about the orig id in the metadata!?
    //  move to smart constructor
  }
  
  def hasName(name: String): Boolean = metadata.hasName(name)
  
}

//---- Scalar -----------------------------------------------------------------

case class Scalar3(get: () => Data = () => Data.empty)
  (metadata: ScalarMetadata)     
  extends Variable3(metadata) {
  
  override def toString: String = s"$getId:${getType.capitalize}"
}

//---- Tuple ------------------------------------------------------------------

case class Tuple3(variables: Seq[Variable3])
  (metadata: TupleMetadata)
  extends Variable3(metadata) {
  
  lazy val arity = variables.length
  
  override def toString: String = variables.mkString(s"$getId:(", ", ", ")")
}

//---- Function ---------------------------------------------------------------

//case class Function3(domain: Variable3, codomain: Variable3)(id: String, metadata: Metadata)
//  only extracts d,c but can't use copy(md = md)...
case class Function3(domain: Variable3, codomain: Variable3)
  (metadata: FunctionMetadata)
  extends Variable3(metadata) {
  
  override def toString: String = s"$getId:($domain -> $codomain)"
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
