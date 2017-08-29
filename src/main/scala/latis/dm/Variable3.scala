package latis.dm

import latis.metadata._
import latis.data.Data
import java.util.UUID
import latis.data.value._

sealed abstract class Variable3(val metadata: VariableMetadata3) {

//  def getMetadata(name: String): Option[String] = metadata.get(name)
//  
//  def getType: String = metadata.getOrElse("type", "undefined") //TODO: require type?
//  
//  lazy val getId: String = getMetadata("id") match {
//    case Some(id) => id
//    case None => UUID.randomUUID.toString.take(8)
//    //TODO: what about the orig id in the metadata!?
//    //  move to smart constructor
//  }
//  
//  def hasName(name: String): Boolean = metadata.hasName(name)
  
}

//---- Scalar -----------------------------------------------------------------

case class Scalar3(get: () => Data = () => Data.empty)
  (metadata: ScalarMetadata)     
  extends Variable3(metadata) 

trait Integer3 { this: Scalar3 => }
object Integer3 {
  def apply(l: Long): Integer3 = {
    val f = () => LongValue(l)
    val md = ScalarMetadata(Map.empty)
    new Scalar3(f)(md) with Integer3
  }
  def unapply(s: Scalar3): Option[Long] = s.get() match {
/*
 * TODO: convert type from native form
 * e.g. AsciiAdapter might still have it as a string
 * use a pattern match in Scalar that uses type from md?
 * construct as one of these types (based on md type)
 *   even if it returns other type
 *   e.g. Interger3(StringValue("1"))
 * Note, the only thing that knows the native type is the Data
 *   so we can't drop Data and just use Any
 *   
 */
    case LongValue(l) => Some(l)
    case _ => None
  }
}

trait Real3 { this: Scalar3 => }
object Real3 {
  def unapply(s: Scalar3): Option[Double] = s.get() match {
    case DoubleValue(d) => Some(d)
    case _ => None
  }
}

trait Text3 { this: Scalar3 => }
object Text3 {
  def unapply(s: Scalar3): Option[String] = s.get() match {
    case StringValue(s) => Some(s)
    case _ => None
  }
}

trait Index3 { this: Scalar3 => }
object Index3 {
  def apply(): Index3 = {
    var index = -1
    val f = () => {
      index += 1
      Data(index)
    }
    val props = Seq("id" -> "index").toMap
    val md = ScalarMetadata(props)
    new Scalar3(f)(md) with Index3
  }
}

//---- Tuple ------------------------------------------------------------------

case class Tuple3(variables: Seq[Variable3])
  (metadata: TupleMetadata)
  extends Variable3(metadata) {
  
  lazy val arity = variables.length
  
}

//---- Function ---------------------------------------------------------------

//continuous function
case class Function3(f: Variable3 => Variable3)
  (metadata: FunctionMetadata)
  extends Variable3(metadata) {
  
  def apply(arg: Variable3): Variable3 = f(arg)
  
}

trait SampledFunction3 { this: Function3 =>
  
  def iterator: Iterator[Sample3]
  
  override def apply(arg: Variable3): Variable3 = ??? //TODO: interpolate, or encapsulate in "f"?
}

object SampledFunction3 {
  
  def apply(samples: Iterator[Sample3])(metadata: FunctionMetadata) = {
    val f: Variable3 => Variable3 = (arg: Variable3) => ??? //TODO: encapsulate interp here, or just override apply?
    new Function3(f)(metadata) with SampledFunction3 {
      def iterator: Iterator[Sample3] = samples
    }
  }

  def unapply(f: SampledFunction3) = Option(f.iterator)
}

case class Sample3(domain: Variable3, range: Variable3)
