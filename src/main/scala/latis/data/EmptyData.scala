package latis.data

import java.nio.ByteBuffer

object EmptyData extends Data {
  def getByteBuffer: ByteBuffer = ByteBuffer.allocate(0)
  def iterator = Iterator.empty
  def getDouble = None
  def getString = None
  def length = 0
  def recordSize = 0
}