package latis.data

import java.nio.ByteBuffer
import latis.data.seq.DataSeq


abstract class IterableData extends Data { //TODO: with Iterable[Data]? problem with 'size' {
  def recordSize: Int
  def iterator: Iterator[Data]
  
  def length: Int = -1 //unknown, potentially unlimited //TODO: iterate and count? IterableOnce problem, cache?
  def size: Int = length * recordSize //total number of bytes
  
  //not a good idea since it realizes all the data, iterate and act on each sample instead
  def getByteBuffer: ByteBuffer = {
     val datas = iterator.toList
     val bb = ByteBuffer.allocate(datas.length * recordSize)
     for (d <- datas) bb.put(d.getByteBuffer)
     bb.flip //set limit and return position to 0
     bb
  }
  
  /**
   * Combine samples from this and another IterableData.
   * Each must have the same length.
   */
  def zip(that: IterableData): IterableData = {
    //TODO: what if both = -1? error if either =-1? or just go till one runs out?
    //println("IterableData zip: " + that.length +" "+ this.length)
    //if (that.length != this.length) throw new Error("zip requires both IterableData-s to be the same length")
    IterableData((this.iterator zip that.iterator).map(p => p._1.concat(p._2)), this.recordSize + that.recordSize)
  }
  
  /**
   * Concatenate the given IterableData onto the end of this one.
   */
  def concat(data: IterableData): IterableData = {
    if (recordSize != data.recordSize) throw new Error("Data samples must be the same size to concatenate IterableData-s.")
    IterableData(iterator ++ data.iterator, recordSize)
  }
  
  /**
   * Convert IterableData to DataSeq. e.g. to avoid iterable once problems.
   */
  def toSeq: DataSeq = DataSeq(iterator.toList)
}

object IterableData {
  
  def apply(it: Iterator[Data], recSize: Int) = new IterableData {
    //TODO: enforce that each sample is the same size?
    //TODO: get size from first sample? PeekIterator?
    def recordSize: Int = recSize
    def iterator: Iterator[Data] = it
  }  
  
  def apply(datas: Seq[Data]) = DataSeq(datas)
  
  def empty = new IterableData {
    def recordSize: Int = 0
    def iterator: Iterator[Data] = Iterator.empty
  }
  
//YAGNI?
//  def apply(peekIterator: PeekIterator[Data]) = new IterableData {
//    def recordSize: Int = peekIterator.peek.size //may invoke data read
//    def iterator: Iterator[Data] = peekIterator
//  }
}