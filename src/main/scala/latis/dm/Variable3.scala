package latis.dm

import latis.metadata._
import latis.data.Data
import java.util.UUID
import latis.data.value._

trait Variable3 {
  def metadata: VariableMetadata3
}

abstract class AVariable3(md: VariableMetadata3) extends Variable3 {

  def metadata: VariableMetadata3 = md

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

trait Scalar3[T] extends Variable3 {
  def value: T
}

//value can be lazy val or def
abstract class AScalar3[T](_value: => T)(metadata: ScalarMetadata)     
  extends AVariable3(metadata) with Scalar3[T] {
  
  //hang onto value after first eval
  def value: T = v
  private lazy val v = _value
}

object Scalar3 {
  
  //Ambiguous due to type erasure Function0[T]
//  def apply(_value: => Long)(md: ScalarMetadata): Integer3 = new Scalar3(_value)(md) with Integer3
//  def apply(_value: => Double)(md: ScalarMetadata): Real3 = new Scalar3(_value)(md) with Real3
//  def apply(_value: => String)(md: ScalarMetadata): Text3 = new Scalar3(_value)(md) with Text3
  
  def apply[T](value: => T)(md: ScalarMetadata) = md.getType match {
  //def apply(value: Any)(md: ScalarMetadata) = md.getType match {
    //TODO: implicit evidence for Long...?
    case "integer" => new AScalar3(value.asInstanceOf[Long])(md) with Integer3
    case "real" => new AScalar3(value.asInstanceOf[Double])(md) with Real3
    case "text" => new AScalar3(value.asInstanceOf[String])(md) with Text3
    case _ => ???
  }
  
  //matching specific type is preferred, avoid cast
  //def unapply(s: Scalar3[_]): Option[Any] = Option(s.value)
}

trait Integer3 extends Scalar3[Long]
object Integer3 {
//  def apply(l: Long): Integer3 = {
//    val props = Seq("id" -> "unknown", "type" -> "integer").toMap
//    val md = ScalarMetadata(props)
//    new Scalar3(l)(md) with Integer3
//  }
  def unapply(s: Integer3): Option[Long] = Option(s.value)
/*
 * TODO: convert type from native form
 * e.g. AsciiAdapter might still have it as a string
 * use a pattern match in Scalar that uses type from md?
 * construct as one of these types (based on md type)
 *   even if it returns other type
 *   e.g. Interger3(StringValue("1"))
 * Note, the only thing that knows the native type is the Data
 *   so we can't drop Data and just use Any
 *   or should the extraction function include the conversion?
 * Do we even need Data?
 *   Spark uses Any (but specifies types in schema)
 * 
 * How will this work with Time
 * still need Real... as traits
 * 
 * With lazy value arg we will need a new instance of Sample3 for each sample
 *   otherwise all samples will end up with the same value
 *   what is the cost of wrapping all samples?
 * The earlier approach of using a function would work
 *   but that adds complications to operations
 *   if backed by "getNext" in adapter, we can call only once
 *     but it does seem dicey to reuse the same Sample instance
 *   getNext could be wrapped by another lazy function in some cases
 *   cases where it can't?
 *     sliding iterator for interpolation?
 *       each element becomes a List[Sample]
 *     parallelization
 *   would a distinction between inner and outer Functions help?
 *     the inner is expected to be memoized (for now, at least)
 *   live with new Sample instances for now
 *     no worse than before  
 *     
 * Should we be able to read then write ascii (string) without parsing values?
 *   writer would ask for the format it wants
 *   just like binary writer wants bytes
 *   match Scalar then call getString or getBytes...?
 *   otherwise it would match on Integer, Real,...
 *   would only work if no ops
 *   seems like an optimization to save for later
 */
}

//TODO: is it worth trying this approach?
//trait Real3 { this: Scalar3[Double] => }
trait Real3 extends Scalar3[Double]
object Real3 {
  def unapply(s: Real3): Option[Double] = Option(s.value)
}

trait Text3 extends Scalar3[String]
object Text3 {
  //def unapply(s: Scalar3[_]): Option[String] = Option(s.value.asInstanceOf[String])
  def unapply(s: Text3): Option[String] = Option(s.value)
}

trait Index3 extends Scalar3[Int]
object Index3 {
  def apply(): Index3 = {
    var index = -1
    def f: Int = {
      index += 1
      index
    }
    val props = Seq("id" -> "index", "type" -> "index").toMap
    val md = ScalarMetadata(props)
    new AScalar3[Int](f)(md) with Index3
  }
  def unapply(s: Index3): Option[Int] = Option(s.value)
}

//---- Tuple ------------------------------------------------------------------

class Tuple3(val variables: Seq[Variable3])
  (metadata: TupleMetadata)
  extends AVariable3(metadata) {
  
  lazy val arity = variables.length
}

object Tuple3 {
  def apply(variables: Seq[Variable3])(properties: Map[String,String] = Map.empty) = {
    val md = TupleMetadata(variables.map(_.metadata))(properties)
    new Tuple3(variables)(md)
  }
  def unapplySeq(tuple: Tuple3): Option[Seq[Variable3]] = Option(tuple.variables)
}

//---- Function ---------------------------------------------------------------

//TODO: continuous function
//class Function3(f: Variable3 => Variable3)
abstract class Function3(metadata: FunctionMetadata)
  extends AVariable3(metadata) {
  
  //def apply(arg: Variable3): Variable3 = f(arg)
}

//trait SampledFunction3 { this: Function3 =>
//class SampledFunction3(f: Variable3 => Variable3)
//TODO: implicit Interpolation, or encapsulate in "f"?
class SampledFunction3(iterable: Iterable[Sample3])
  (metadata: FunctionMetadata)
  extends Function3(metadata) {
  
  def iterator: Iterator[Sample3] = iterable.iterator
}
/*
 * TODO: consider inner SampledFunction
 *   no need for iterator?
 *   avoid traversable once problem?
 */

object SampledFunction3 {
  
  def apply(samples: Iterable[Sample3])(metadata: FunctionMetadata) = {
    new SampledFunction3(samples)(metadata)
  }
  
  def apply(samples: Iterator[Sample3])(metadata: FunctionMetadata) = {
    val it = new Iterable[Sample3]() {
      def iterator = samples
    }
    new SampledFunction3(it)(metadata)
  }

  def unapply(f: SampledFunction3) = Option(f.iterator)
}

case class Sample3(domain: Variable3, range: Variable3)
