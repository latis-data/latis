package latis.data.seq

import latis.data.Data
import latis.data.IterableData

import scala.collection.Seq
import scala.collection.mutable

/**
 * Data that represents multiple data records.
 * Not multiple data values in a single record.
 * Each element is assumed to have the same type and size.
 * Note, this is different from *SeqData which encapsulate a sequence of primitive values.
 */
class DataSeq extends IterableData {
  
  private val datas = mutable.ArrayBuffer[Data]()
  
  override def length: Int = datas.length  
  
  private var _recordSize = -1 //bytes per record
  def recordSize: Int = _recordSize
  
  def apply(index: Int): Data = datas(index)
  
  //def iterator = datas.iterator
  def iterator: Iterator[Data] = datas.toList.iterator  //test if this will behave better from a List, yep!?
  
  def append(data: Data): DataSeq = {
    val bb = data.getByteBuffer
    val size = bb.limit
    
    //all samples must have same size as the first
    if (recordSize < 0) _recordSize = size
    else if (recordSize != size) throw new Error("IterableData requires that each Data record has the same size.")
    
    datas += data //append to collection
    this
  }
  
  def concat(data: DataSeq): DataSeq = {
    DataSeq(datas ++ data.datas)
  }
  
  def zip(that: DataSeq): DataSeq = {
    if (that.length != this.length) throw new Error("zip requires both DataSeq-s to be the same length")
    DataSeq((this.datas zip that.datas).map(p => p._1.concat(p._2)))
  }
  
  //override def toSeq: Seq[Data] = datas.toList
}

object DataSeq {
  def apply(): DataSeq = new DataSeq()
  
  def apply(data: Data): DataSeq = (new DataSeq()).append(data) 
  
  def apply(datas: Seq[Data]): DataSeq = datas.foldLeft(new DataSeq())(_ append _)
  //Note, will fail if not all elements are the same size
  
  def apply(datas: Iterator[Data]): DataSeq = DataSeq(datas.toSeq)
  
  /**
   * Make a DataSeq out of a single Data by breaking it up by the given record size.
   */
  def apply(data: Data, recordSize: Int): DataSeq = {
    DataSeq(data.getBytes.grouped(recordSize).map(Data(_)).toSeq)
  }
}