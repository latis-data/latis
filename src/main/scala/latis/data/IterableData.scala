package latis.data

import java.nio.ByteBuffer

/**
 * Abstract data implementation designed for Adapters to implement.
 */
abstract class IterableData extends Data {
  //TODO: trait?
  //TODO: should all Data extend this since they impl iterator?
  //TODO: or make DataIterator? complication with "length"
  //TODO: should Data even define iterator method or do it here?
  
  /*
   * was use for adapters and such to impl 
   * as opposed to defining concrete subclasses
   * could we use this with "recordSize" and "append"?
   * SeqData uses recordSize
   * should we use SeqData instead?
   *   but it was designed for use by scalars with all the data
   *   should deprecate that
   *   use implicit Index Function instead
   */
  
  
  //To get record/sample by index, need to manage iterator ourselves, as Stream.
  //TODO: review use of Stream in light of caching
  lazy val stream: Stream[Data] = iterator.toStream
  def apply(index: Int): Data = stream(index)
  
  def length = -1 //unknown
  //TODO: support unknown length, "-n" where n is current length?
  
  //TODO: should "get" advance to next?
  //def getDouble: Option[Double] = ???
  //def getString: Option[String] = ???
  def getByteBuffer: ByteBuffer = ???
  
  //TODO: abstract up iterator logic
  //  def recordIterator, recordToData?
}