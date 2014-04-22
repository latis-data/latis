package latis.data

import java.nio.ByteBuffer
import scala.collection.mutable


abstract class IterableData extends Data { //TODO: with Iterable[Data] {
  def recordSize: Int
  def length: Int = -1 //unknown, potentially unlimited //TODO: iterate and count?
  def size: Int = length * recordSize //total number of bytes
  def isEmpty: Boolean = length == 0
  
  def iterator: Iterator[Data]
  def getByteBuffer: ByteBuffer = {
     val datas = iterator.toList
     val bb = ByteBuffer.allocate(datas.length * recordSize)
     for (d <- datas) bb.put(d.getByteBuffer)
     bb
  }
}

//TODO: deprecate SeqData?

class DataSeq extends IterableData {
  private val datas = mutable.Seq[Data]()
  
  override def length: Int = datas.length  
  
  private var _recordSize = -1 //bytes per record
  def recordSize: Int = _recordSize
  
  def apply(index: Int): Data = datas(index)
  
  def iterator = datas.iterator
  
  def append(data: Data): DataSeq = {
    val bb = data.getByteBuffer
    val size = bb.limit
    
    //all samples must have same size as the first
    if (recordSize < 0) _recordSize = size
    else if (recordSize != size) throw new Error("IterableData requires that each Data record has the same size.")
    
    datas :+ data //append to collection
    this
  }
}

object DataSeq {
  def apply() = new DataSeq()
  
  def apply(data: Data) = (new DataSeq()).append(data) 
  
  def apply(datas: Seq[Data]) = datas.foldLeft(new DataSeq())(_ append _)
  //Note, will fail is not all elements are the same size
}



/**
 * Abstract data implementation designed for Adapters to implement.
 */
abstract class IterableDataORIG {//extends Data {
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
//  lazy val stream: Stream[Data] = iterator.toStream
//  def apply(index: Int): Data = stream(index)
  
  def length = -1 //unknown
  //TODO: support unknown length, "-n" where n is current length?
  
  //TODO: should "get" advance to next?
  //def getDouble: Option[Double] = ???
  //def getString: Option[String] = ???
  def getByteBuffer: ByteBuffer = ???
  
  //TODO: abstract up iterator logic
  //  def recordIterator, recordToData?
}