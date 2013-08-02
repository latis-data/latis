package latis.data

import latis.dm._
import latis.ops.math._
import java.nio.ByteBuffer
import latis.data.value._
import latis.data.buffer.ByteBufferData

/*
 * TODO: 2013-05-30
 * simplifying assumptions:
 *   ok for scalars to have array Data
 *     like column oriented database
 *     implicit IndexFunction
 *     FunctionIterator can stitch them together
 *     Arrays or ByteBuffers?
 *   outer function can have Data (Iterable) to support sample iteration
 *     ByteBuffer
 *     what about Text? IndexFunction of Chars?
 *   don't worry about iterating into nested functions, yet
 *   embrace sample/record abstraction (outer function)
 *   
 */

/*
 * TODO: WrappedData?
 * stride for subset, used during access, on top of orig data
 */

trait Data extends Any {
  //TODO: rawData? that's how value is being used: the constructor arg, akin to scala collection repr?
  //TODO: lazy vals instead of def?
  
  //defaults assume Data with a single Double.NaN
  //TODO: make them abstract?
  
  def getByteBuffer: ByteBuffer 
//  = {
//    val d = doubleValue
//    ByteBuffer.allocate(8).putDouble(d).flip.asInstanceOf[ByteBuffer]
//  }
  
  
  def doubleValue: Double = getDouble match {
    case Some(d) => d
    case None => throw new Error("No Data") //null
  }
  
  def stringValue: String = getString match {
    case Some(s) => s
    case _ => throw new Error("No Data")
  }
    
  def getDouble: Option[Double] //= None
  def getString: Option[String] //= None
  
  //TODO: word = Array of 4 chars, 8 bytes
  //TODO: String as Index array of Char, or Word?
  //TODO: Blob: fixed length byte array

  //TODO: beware of mixing getters that increment with iterator
  def iterator: Iterator[Data] //= List(DoubleValue(doubleValue)).iterator

  //TODO: support foreach, (d <- data)
//  def foreach(f: (Data) => Unit) = {
//    val it = iterator
//    while (it.hasNext) f(it.next)
//  }
  
  def isEmpty: Boolean = length == 0
  def notEmpty = ! isEmpty
  
  //def apply(index: Int): Any = value if 0 else IOOB?
  
  //TODO: head::tail semantics? Stream?
  
  //notion of record, size (bytes), length (number of records)
  //  can Data know about records, or do we need model?
  //  seems like we should capture at least that much when constructing Data, e.g. support iteration of records
  //avoid having to put length in metadata?
  
  def length: Int //= 1 //number of records
  def recordSize: Int //= 8 //bytes
  def size = length * recordSize
  
  //TODO: is byte buffer equality sufficient?
  override def equals(that: Any) = that match {
    case d: Data => d.getByteBuffer == getByteBuffer
    case _ => false
  }
  
  override def hashCode = getByteBuffer.hashCode
}

//-----------------------------------------------------------------------------

object Data {
  
  val empty = EmptyData
  
  def apply(d: Double): Data = DoubleValue(d)
  def apply(s: String): Data = StringValue(s)
  def apply(ds: Seq[Double]): Data = SeqData(ds)
  
  def apply(bb: ByteBuffer): Data = new ByteBufferData(bb, bb.limit) //one sample
  
  //Concatenate Data
  //"flip" to reset to the beginning and set the "limit" to the actual size
  //implicit hack for type erasure ambiguity
  def apply(data: Seq[Data])(implicit ignore: Data): Data = {
    val size = data.foldLeft(0)(_ + _.size)
    val bb = data.foldLeft(ByteBuffer.allocate(size))(_ put _.getByteBuffer).flip.asInstanceOf[ByteBuffer]
    Data(bb)
  }
  
//  def apply(dit: Iterator[Data]) : Data = new Data {
//    override def iterator = dit
//    //TODO: deal with recordSize, length..., unknown
//  }
}
//
////class DoubleDatum(val value: Double) extends AnyVal with Data with Datum {
////note, Double instead of Real to keep it closer to the implementation
//class DoubleDatum(double: Double) extends NumericDatum(double) {
//  def toDouble = double
//  def toLong = double.toLong
//}
////  //TODO: implicit def doubleToDatum(value: Double): DoubleDatum = Datum(value)
////  //  put in latis.data package object?
//
//class LongDatum(long: Long) extends NumericDatum(long) {
//  def toDouble = long.toDouble
//  def toLong = long
//}







//=============================================================
  //TODO: extends Iterable[T] but introduces type hell
  //  just add iterator method for now



  /*
   * What should Data look like?
   * ScalarData could be a simple value class, wrap AnyVal
   *   but what about String? it does work!
   *   class Text(val s: String) extends AnyVal
   *   
   * do we need anything beyond RealData, IntegerData...?
   * do we need TimeData or can we encapsulate time specific stuff in Time (extends Real)?
   *   probably, need metadata (e.g. units) to support time semantics
   *   data for Reals only needs to know it is a Double
   *   basic math could work on Data?
   *     depends on how we do TupleData and FunctionData
   *     should they have parentage?
   *     we should make this work
   *     could take a shortcut is tuple has the data (e.g. interleved)
   *     function only work now by exposing domain and range sets!
   *     needs to act on iterator if Function "has the data"
   *   math with units... would have to work on Variables
   * Data could be sealed case classes
   * Is it worth making them value classes?
   *   pattern matching/unapply will require instantiation anyway
   * what about Complex, Matrix,...
   *   
   * Variable.unapply can expose md and data, just like default case classes, but we do want to be able to extend Vars
   * 
   * Consider extending scala.math.Numeric?
   * but could our math work with any scala Numeric? 
   * BigDecimal is not Numeric, it is SalaNumber with ScalaNumberConversions
   *   ScalaNumber not in public API?
   *   trait ScalaNumericConversions extends ScalaNumber with ScalaNumericAnyConversions 
   */





//trait Number extends Any
////TODO: might be nice to extend Data but will break Value classes
////TODO: Numeric? NumericData?
//
////TODO: case classes for now (obj apply and unapply), but will want to extend later
////  but pattern matching will negate benefit of value classes, oh well, so don't let that get in the way
//case class RealData(val value: Double) extends AnyVal with Data with Number {
//  override def toString() = value.toString
//}
//case class IntegerData(val value: Long) extends AnyVal with Data with Number
//case class IndexData(val value: Int) extends AnyVal with Data  with Number
//
//case class TextData(val value: String) extends AnyVal with Data
//
////following can't be value classes
//
//class ComplexData(val real: Float, val imaginary: Float) extends Data with Number
//
//class MatrixData(val values: Array[Array[Double]]) extends Data with Number
//
/*
 * TODO: TupleData
 * Array[Any]? Array[Data]? or param T, so we could have all doubles and get primative array for all Reals?
 * Array[Data] is probably best for now, the rest is optimization
 */

/*
 * TODO: FunctionData
 * math currently exposes complete domain set and range
 * we should support iteration of samples
 * can't quite do that with domain and range unless we zip them
 * we currently manage much of this in Function
 * but it might be cleaner if we do it in FunctionData
 * 
 * what would happen if FunctionData is empty, delegate to kids
 * but no sense of Seq?
 * but even in tsml, we can define a scalar with multiple values that becomes an IndexFunction
 * allow ScalarData to be and array???
 *   sounds attractive, but we started there some time ago
 *   maybe metadata separation will let it work
 *   and also for TupleData?
 * or should FunctionData contain DomainSet and RangeSet?
 * back to parentage question, can't simply say if FunctionData is empty, go to domain and range (e.g. scalars) independently
 * 
 * What about nested Functions, SSI
 * would be nice for inner function to manage it's own data instead of glomming it into each outer function sample
 * we really could use fully recursive behavior
 * 
 * Array or Iterable?
 * might as well do Iterable for now
 * even ScalarData could be iterable
 * extend Iterable? then need to deal with types...
 * 
 * Or should data simply be a byte array?
 * but want to iterate
 * byte stream?
 * 
 * ++what should the Data API be?
 * replaceable
 * iterator
 * apply(index)
 * will iterator.toIterable work for "stream" or do we need to do our own?
 * 
 * look at math and consider how we can avoid making a Scalar for every sample
 * using Data or even primitives within Data
 * what should TupleData and FunctionData look like to support math
 */

//class ScalarData[T](val value: T) extends Data {
//  def iterator = List(value).iterator //is there a more efficient way? use our own, like FunctionIterator?
//  
//}
//
//case class RealData(data: Double) extends ScalarData[Double](data) 



//case class TupleData(data: Seq[Data]) extends Data
/*
 * but this is the same as delegating to kids?
 * do we still need to do math on vars and not have Data manage parentage?
 * seems like overlapping concerns
 * tuple data should simply be interleaved data, know nothing about the kids
 * would be nice to take advantage of primitives
 * tempted to do Array[AnyVal] but still need iterable
 * or back to byte array, ByteBuffer?
 * see previous latis impl: LaTiS_0 DatasetAccessor
 *   doubleIterator, tupleIterator,...
 * 
 * should Data just be a ByteBuffer?
 * adapter doesn't have to load it all at once
 * maintain the tie to the adapter so it can keep feeding the buffer
 * not a problem since the adapter constructs the Var with the Data object
 * what is the cost going to/from bytes vs storing Doubles,...?
 * 
 * do we need subtypes of Data?
 * prob not
 * String as CharSequence, CharBuffer
 * CharIterator? nextString?
 * 
 * 
 * **Data API
 *   doubleIterator
 *   longIterator
 *   ...
 *   for (d <- data) ...
 *   don't need tupleIterator because type hierarchy can tell us how many to read, and get the types right
 *     besides, what would it be? an array of bytes?
 *     may be useful in the context of function samples
 *   generic iterator?
 *     need to know types to get number of bytes
 *     would it return Datum[T]?
 *   
 * EmptyData
 * Datum
 *   extends Data, but has only one element
 *   value class?
 *   to/get Double...
 *   use when constructing with a value: Real(md, Datum(3.14))
 *   Datum[T]? or RealDatum or DoubleDatum?
 *   is this useful?
 *   no need for byte conversion
 * 
 * Variable.getData
 *   will present the data as if that Var was constructed with it
 *   e.g. tuple.getData => interlace data from kids
 * 
 */

/*
 * TODO: should Number be part of Variable or Data hierarchy?
 * sounds like math should be in terms of Vars since it needs function relationships...
 * Number sounds like an instance
 * Numeric Variable seems to work
 * so use Number in the Data hierarchy
 * 
 * consider Text, TextData, StringValue,...
 *   NumberData? or NumericData?
 *   Number implies one so this makes sense for toDouble...
 *   String is not AnyVal, can be treated as a seq of chars
 *   so Text* already implies more than one element
 *   StringValue does well to express a single String
 * Number is the base trait for numeric value classes: DoubleValue,...
 * so StringValue is probably the best name for the Text equivalent
 */


