package latis.dm

import latis.metadata._
import scala.collection._
import scala.collection.mutable.Stack
import scala.collection.mutable.Builder
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ArrayBuffer

case class Dataset3(variable: Variable3)(metadata: Metadata3)
  extends Traversable[Variable3] with TraversableLike[Variable3, Dataset3]{
  //This implementation should not contain any data. It should be plugged in
  // by the adapter via the Scalar.unapply.
  //TODO: common trait so Dataset and Variables can share metadata methods?
  
  import Dataset3._
  override protected[this] def newBuilder: Builder[Variable3, Dataset3] = Dataset3.newBuilder
  
  /**
   * Implement Traversable so we can use filter....
   */
  def foreach[U](f: Variable3 => U): Unit = {
    def go(v: Variable3): Unit = {
      //recurse
      v match {
        case _: Scalar3     => //end of this branch
        case Tuple3(vs)     => vs.map(go(_))
        case SampledFunction3(it) => it foreach { sample =>
          //traverse over samples
          go(sample._1)
          go(sample._2)
        }
      }
      //apply function after taking care of kids = depth first
      f(v)
    }
    go(variable)
  }

  def getScalars: Seq[Scalar3] = toSeq.collect { case s: Scalar3 => s }
  
  def findVariableByName(vname: String): Option[Variable3] = {
    //TODO: support "." in vname, see AbstractVariable
    find(_.hasName(vname))
  }
  
  def findVariableAttribute(vname: String, attribute: String): Option[String] = {
    findVariableByName(vname).flatMap(_.getMetadata(attribute))
  }
  
  def getId = metadata.properties.get("id").get //TODO: error if not defined
  
  override def toString: String = s"$getId: ($variable)"
  
//See how far Traversable gets us
//  /**
//   * Map a function over the Dataset returning a new Dataset
//   * Encapsulate the recursion so the function doesn't have to.
//   * The function will be applies to a Variable after its kids = depth first.
//   */
//  /*
//   * TODO: note that f could try to extract data
//   * since these are f(v) we don't need to extract value anyway
//   *   maybe a Variable traversal with f: A=>B might?
//   * no need for special map for modle
//   * Consider V => Option[V]
//   */
//  def map(f: Variable3 => Variable3): Dataset3 = {
//    //TODO: update metadata
//    val newVariable = this match {
//      case Dataset3(v, md) => v match {
//        case s: Scalar3 => f(s)
//        case Tuple3(id, md, vs @ _*) => f(Tuple3(id, md, vs.map(f(_)): _*))
//        case Function3(id, md, d, c) => f(Function3(id, md, f(d), f(c)))
//      }
//    }
//    this.copy(variable = newVariable)
//  }
  

  
  /*
   * Dataset:
   * as just the model, data via unapply via adapter.getNextData(vname)
   * do we need DatasetModel (e.g.. from tsml) 1st?
   *   reader needs to construct adapter
   *   adapter can be reused (tsml vs lemr)
   * Could we use Dataset[T] where T is Nothing for the model without data?
   *   type alias DatasetModel?
   *   what might T be when complete? source type (e.g. Netcdf)? 
   *   Monadic properties? 
   *   Free monad? http://typelevel.org/cats/datatypes/freemonad.html
   *     T seems to be the type that comes out of a "program"
   *     we always return a Dataset
   *   T as the adapter that can interpret the model and fill it with data?
   *     still akin to Free monad?
   *     ADT could be the variables
   *     the model for a specific dataset is the program
   *     the adapter interprets the program
   *     then make another algebra for ops and compose them?
   * construct Scalar with function that returns value? "=> Data"
   *   instead of the actual value
   *   unapply could use it
   *   is this any different that calling the adapter method?
   *   problem navigating back to adapter
   *   could just lift the method?
   * mix in traits like Real
   *   object Real unapply extracts Double...?
   * we should be able to match on Variable3 to extract id and metadata
   * Function(d,c), SampledFunction(it) iterator of (d,c) pairs
   *   no need for Sample? Maybe Tuple?
   *   but if SampledFunction is a Seq of Samples = Tuples = Variables = not special
   * 
   * Traversable vs Functor (map):
   * need diff traversals: map, mapData
   *   but we need the "foreach" (e.g. for writer)
   * Variable vs Dataset
   *   we have 2 diff traversals: model, data
   *   can we use Dataset and Variable to handle those separately?
   *   variable as Functor, Dataset as Free monad?
   * map over function of V=>V, V=>O[V], or A=>B?
   *   v => O[V] would support filter
   *   flatMap?
   * DSL: everything is a Dataset
   *   define map, filter, flatMap?, mapData...
   *   make V traversable? but need diff traversals for model vs data...
   * define map on Dataset or Variable?
   *   V.map(A => B)
   *   D.map(V => V)
   *   D.filter(V => Boolean)
   * Traversable doesn't allow us to preserve Dataset metadata  
   * ***Does 2 diff traversals call for 2 diff structures?
   *   Model and Dataset?
   *   encapsulate model with metadata
   *   put the model traversal on Metadata
   *     start the traversal in Dataset so it can replace the orig
   *   is "metadata" the right abstration?
   *     the thing from tsml or lemr,... seems OK
   *   Do Variables need their own metadata?
   *     convenient
   *     var has no parentage to get to dataset's md
   *       but it is in scope because we are within the context of the Dataset
   *       ...
   *     could just point to the part from the DS metadata
   *   Will help us write metadata without reading data 
   *   Note that spark has a "type" in the field definitions
   *     they also have metadata
   *   Do we need Metadata(VariableMetadata) wrapper? only way I could get Traversable to work
   *     Traversable becomes a Seq[A] but the thing is an A itself
   * 
   * Extractors:
   * can we use primitives instead of Data? 
   *   value classes should be ok
   *   but if we have functions of Double => Double ...
   * 
   * Metadata:
   * in all vars or just Dataset?
   *   consider ease of changing units...
   *   need to do math and update metadata
   * 
   * DatasetSource:
   * fromName, fromURL
   * needs to know tsml vs tsml3 vs lemr vs netcdf...
   * +++
   * 
   * Adapter:
   * "config" instead of "properties"?
   * construct with PIs instead of putting them in model?
   *   or could PIs be part of adapter config?
   *   since we wouldn't be imited to name,value pairs we could encode a Seq of PIs
   * How might this affect joins, file joins,...
   * 
   * 
   * 
   * Operations:
   * pure function: Variable => Variable
   *   or Variable => Option[Variable]?
   *   or diff types, e.g. filter: Variable => Boolean
   * Where can an operation update Dataset metadata?
   */
}

object Dataset3 {
  
//  def fromSeq(vars: Seq[Option[V]]): Model = {
//    def go(vs: Seq[Option[V]], hold: Stack[Option[V]]): Option[V] = {
//      vs.headOption match {
//        case Some(s: Option[S]) => go(vs.tail, hold :+ s)
//        case Some(ot: Option[T])  => ot match {
//          case Some(t) =>
//            val n1 = t.variables.length
//            val tvs = hold.take(n1).flatten //drop Nones
//            val n2 = tvs.length
//            val newT = if (n2 == 0) None
//              else if (n2 == 1) Option(tvs.head)
//              else Option(TupleType(tvs))
//            go(vs.tail, hold.drop(n1).push(newT))
//          case None => ot
//        }
//       // case
//        case None => hold.pop //done
//      }
//      
//      ???
//    }
//    
//    go(vars, Stack[Option[V]]()) match {
//      case Some(v) => Model(v)
//      //case None    => Model() //TODO: empty model
//    }
//  }
  
  def fromSeq(vars: Seq[Variable3]): Dataset3 = {
    ??? //TODO: traverse over data samples
//    def go(vs: Seq[Variable3], hold: Stack[Variable3]): Variable3 = {
//      vs.headOption match {
//        case Some(s: Scalar3) => 
//          go(vs.tail, hold.push(s)) //put on the stack then do the rest
//        case Some(t: Tuple3) => 
//          val n = t.arity
//          val t2 = t.copy(variables = (0 until n).map(_ => hold.pop).reverse)
//          go(vs.tail, hold.push(t2))
//        case Some(f: Function3) => 
//          val c = hold.pop
//          val d = hold.pop
//          val f2 = f.copy(domain = d, codomain = c)
//          go(vs.tail, hold.push(f2))
//        case None => hold.pop //TODO: test that the stack is empty now
//      }
//    }
//    //Note, this doesn't preserve dataset metadata
//    Dataset3(go(vars, Stack()))(Metadata3.empty)
  }
  
  def newBuilder: Builder[Variable3, Dataset3] = new ArrayBuffer mapResult fromSeq

  implicit def canBuildFrom: CanBuildFrom[Dataset3, Variable3, Dataset3] =
    new CanBuildFrom[Dataset3, Variable3, Dataset3] {
      def apply(): Builder[Variable3, Dataset3] = newBuilder
      def apply(from: Dataset3): Builder[Variable3, Dataset3] = newBuilder
    }

}

