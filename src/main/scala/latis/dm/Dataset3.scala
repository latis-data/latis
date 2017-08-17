package latis.dm

import latis.metadata.Metadata

case class Dataset3(variable: Variable3, metadata: Metadata = Metadata.empty) {

//  def map[A, B](f: A => B): V[B] = this match {
//    case S(v: A) => S(f(v))
//    case T(vs @ _*) => T(vs.map(_.map(f)): _*)
//    case F(d,c) => F(d.map(f), c.map(f))
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
   * Variable vs Dataset
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
   *   
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
   */
}