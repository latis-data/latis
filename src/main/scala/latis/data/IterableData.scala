package latis.data

import java.nio.ByteBuffer
import scala.collection.mutable


abstract class IterableData extends Data { //TODO: with Iterable[Data]? problem with 'size' {
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
