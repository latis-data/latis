package latis.reader.tsml

import latis.dm._
import scala.io.Source
import scala.collection._
import latis.data.Data
import latis.util.Util
import java.nio.ByteBuffer
import latis.data.IterableData
import latis.time.Time
import latis.reader.tsml.ml.Tsml

class IterativeAsciiAdapter(tsml: Tsml) extends IterativeAdapter(tsml) with AsciiParser {
  
  def makeIterableData(sampleTemplate: Sample): Data = new IterableData {
    def recordSize = sampleTemplate.getSize
    val recordIterator = getRecordIterator
    
    //TODO: make PeekIterator? but don't access data source prematurely
    def iterator = new Iterator[Data] {
      override def hasNext = recordIterator.hasNext
      override def next = {
        val record = recordIterator.next
        val svals = parseRecord(record)
        makeDataFromRecord(sampleTemplate, svals)
      }
    }
  }
  
  //TODO: compare to Util dataToVariable... move this there?
  def makeDataFromRecord(sampleTemplate: Sample, svals: Map[String, String]): Data = {
    //build a ByteBuffer
    val size = sampleTemplate.getSize
    val bb = ByteBuffer.allocate(size)
    
    //assume every Scalar in the template has a value in the Map, e.g. not stored by Tuple name
    //get Seq of Scalars from template
    val vars = sampleTemplate.toSeq 
    
    for (v <- vars) {
      val s = svals(v.getName) //string value for the given scalar
      v match {
        case _: Real => bb.putDouble(s.toDouble)
        case _: Integer => bb.putLong(s.toLong)
        case t: Text => {
          val padded = "%"+t.length+"s" format s //pad to the Text variable's defined length
          s.foldLeft(bb)(_.putChar(_)) //fold each character into buffer
        }
      }
    }
    
    //rewind for use
    Data(bb.flip.asInstanceOf[ByteBuffer])
  }

}