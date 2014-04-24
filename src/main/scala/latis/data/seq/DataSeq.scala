package latis.data.seq

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.DoubleValue
import latis.data.Data
import latis.data.IterableData

/**
 * Data that represents multiple data records.
 * Not multiple data values in a single record.
 * Each element is assumed to have the same type and size.
 */
class DataSeq extends IterableData {
  //TODO: refactor to work with other SeqData?
  //Data vs Datum
  
  private val datas = mutable.ArrayBuffer[Data]()
  
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
    
    datas += data //append to collection
    this
  }
  
  def zip(that: DataSeq): DataSeq = {
    if (that.length != this.length) throw new Error("zip requires both DataSeq-s to be the same length")
    DataSeq((this.datas zip that.datas).map(p => p._1.concat(p._2)))
  }
}

object DataSeq {
  def apply() = new DataSeq()
  
  def apply(data: Data) = (new DataSeq()).append(data) 
  
  def apply(datas: Seq[Data]) = datas.foldLeft(new DataSeq())(_ append _)
  //Note, will fail if not all elements are the same size
}