package latis.dm

import latis.metadata._
import scala.collection._
import scala.collection.mutable.Stack
import scala.collection.mutable.Builder
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ArrayBuffer

case class Dataset3(function: SampledFunction3)(val metadata: Metadata3)
  extends Traversable[Sample3] with TraversableLike[Sample3, Dataset3]{
  //This implementation should not contain any data. It should be plugged in
  // by the adapter via the Scalar.unapply.
  //TODO: common trait so Dataset and Variables can share metadata methods?
  
  import Dataset3._
  override protected[this] def newBuilder: Builder[Sample3, Dataset3] = Dataset3.newBuilder
  
  /**
   * Implement Traversable so we can use filter....
   */
  def foreach[U](f: Sample3 => U): Unit = {
    function.iterator.foreach(f)
  }

//  //def getScalars: Seq[Scalar3] = toSeq.collect { case s: Scalar3 => s }
//  
//  def findVariableByName(vname: String): Option[Variable3] = {
//    //TODO: support "." in vname, see AbstractVariable
//    find(_.hasName(vname))
//  }
//  
//  def findVariableAttribute(vname: String, attribute: String): Option[String] = {
//    findVariableByName(vname).flatMap(_.getMetadata(attribute))
//  }
  
  def getId = metadata.properties.get("id").get //TODO: error if not defined
  
  override def toString: String = s"$getId: (${metadata.metadata})"
  
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
   * Should Dataset be a traversable of Samples
   *   then should operations be f: Sample=>Sample
   *   obvious place for Left,Right
   *   can use metadata to traverse the sample?
   *   maps well to spark Dataset[Row]
   * 
   * Extractors:
   * can we use primitives instead of Data? 
   *   value classes should be ok
   *   but if we have functions of Double => Double ...
   * instead of ()=>Double in Scalar, allow f to take args?
   *   e.g. cursor info so adapter doesn't have to manage cursor?
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
   * Data:
   * always use value class?
   * with base class with asSting, asDouble, asLong,...
   * should we just use primitive? use type from metadata to interpret?
   *   Any is good enough for Spark
   * 
   * Operations:
   * pure function: Variable => Variable
   *   or Variable => Option[Variable]?
   *   or diff types, e.g. filter: Variable => Boolean
   * Where can an operation update Dataset metadata?
   * 
   * Time:
   * 
   */
  
  /*
   * Another look at the model impl
   * metadata and data symetry
   * avoid separate Metadata and Dataset structures?
   * or combine them in one?
   *   Dataset(Metadata, Data)
   *   Variable as VariableMetadata vs VariableData?
   * Or back to Variable(Metadata, Data)?
   *   VariableData as node of Dataset Data, like Metadata?
   *   
   * Consider use case: ds.map(foo + 1)
   *   data traversal
   *   scalar.hasName("foo")
   *     can we do this once at the metadata traversal level?
   *     get ID for name..., could also resolve long name of nested tuples
   *   or add operation to Seq attached to "metadata" model
   *     apply at end of world - only time we can traverse data
   *     treat like we would processing instructions
   *   define operation once per variable, not for each sample
   *   attach operations to the Variable it acts on instead of the entire dataset?
   *     Op: V -> V
   *     what about provenance?
   *     still need to traverse and build new Dataset (model/metadata) to add Operation
   *     can compose operations before adding them to the dataset
   *     compile, optimize
   *   complicated by laziness
   *     many approaches seem equivalent
   *     don't be in a big hurry to "apply" operations - wait until write
   *   should Sample be pair of Variables or just data?
   *     we don't want to have to apply logic to every sample if we don't have to
   *       e.g. hasName("foo")
   *   now: scalar extractor returns value, can lazily wrap in another function
   *     at the value type level, not Variable
   *     function can encapsulate unit conversion, generate f once
   *   Ops at Dataset level [Variable]
   *     inside Dataset use functions of T?
   *     dsOps: if v.hasName("foo")...  makeUnitConversion
   *     fooOps: v * s + o + 1
   *     minimize work to do for each sample
   */
}

object Dataset3 {
  
  def fromSeq(samples: Seq[Sample3]): Dataset3 = {
    val (dmd, cmd) = samples.head match {
      case Sample3(d, c) => (d.metadata, c.metadata)
    }
    val props = Seq(("length" -> samples.length.toString)).toMap
    val fmd = FunctionMetadata(dmd, cmd)(props)
    Dataset3(SampledFunction3(samples.iterator)(fmd))(Metadata3.empty)
  }
  
  def newBuilder: Builder[Sample3, Dataset3] = new ArrayBuffer mapResult fromSeq

  implicit def canBuildFrom: CanBuildFrom[Dataset3, Sample3, Dataset3] =
    new CanBuildFrom[Dataset3, Sample3, Dataset3] {
      def apply(): Builder[Sample3, Dataset3] = newBuilder
      def apply(from: Dataset3): Builder[Sample3, Dataset3] = newBuilder
    }

}

