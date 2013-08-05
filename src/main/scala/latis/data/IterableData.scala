package latis.data

import java.nio.ByteBuffer

/**
 * Abstract data implementation designed for Adapters to implement.
 */
abstract class IterableData extends Data {
  //TODO: trait?
  //TODO: should all Data extend this since they impl iterator?
  //TODO: or make DataIterator? complication with "length"
  
  def length = -1 //unknown
  //TODO: support unknown length, "-n" where n is current length?
  
  //TODO: should "get" advance to next?
  //def getDouble: Option[Double] = ???
  //def getString: Option[String] = ???
  def getByteBuffer: ByteBuffer = ???
  
  //TODO: abstract up iterator logic
  //  def recordIterator, recordToData?
}