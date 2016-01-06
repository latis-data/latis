package latis.reader.tsml

import latis.reader.tsml.ml._
import java.nio.ByteBuffer
import java.io.RandomAccessFile
import latis.dm.Function
import latis.dm.Dataset
import latis.data.Data
import latis.util.iterator.PeekIterator
import latis.dm.Sample
import latis.util.DataUtils

/**
 * Read a binary dataset with a RandomAccessFile one sample at a time.
 */
class BinAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  private lazy val reader = new RandomAccessFile(getUrlFile, "r")
  
  def getRecordIterator: Iterator[ByteBuffer] = {
    new Iterator[ByteBuffer]() {
      //Get buffer size in bytes - one record
      val bufferSize = getOrigDataset match {
        case Dataset(f: Function) => f.getDomain.getSize + f.getRange.getSize
      }
      //reuse ByteBuffer
      val bytes = new Array[Byte](bufferSize) //this will be the live backing array
      val bb = ByteBuffer.wrap(bytes)
      lazy val fileLength = reader.length
      
      /**
       * Is there enough bytes left in the file to fill the next buffer?
       */
      def hasNext: Boolean = {
        fileLength - reader.getFilePointer >= bufferSize
      }
      
      def next: ByteBuffer = {
        bb.clear //reset position to 0...
        reader.read(bytes) //read data into ByteBuffer's backing array
        bb
      }
    }
  }
  
  override def makeFunction(f: Function): Option[Function] = {
    val template = Sample(f.getDomain, f.getRange)
    val samples = getRecordIterator.map(bb => {
      DataUtils.dataToSample(Data(bb), template)
    })
    Some(Function(template.domain, template.range, samples))
  }
  
  override def close = reader.close
}