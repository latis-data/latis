package latis.data

import java.nio.ByteBuffer


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
     bb
  }
}

object IterableData {
  
  def apply(it: Iterator[Data], recSize: Int) = new IterableData {
    def recordSize: Int = recSize
    def iterator: Iterator[Data] = it
  }  
  
//YAGNI?
//  def apply(peekIterator: PeekIterator[Data]) = new IterableData {
//    def recordSize: Int = peekIterator.peek.size //may invoke data read
//    def iterator: Iterator[Data] = peekIterator
//  }
}