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

class IterativeAsciiAdapter(tsml: Tsml) extends IterativeAdapter(tsml) {
  
  //handy aliases for a String when we are using it in the context of variable names and values.
  //TODO: could we use the same approach for binary data? define these as ByteBuffer instead of String?
  
  /**
   * Used for the name of a Variable as defined by the "name" attribute in the TSML, presumably a Scalar.
   */
  type Name = String
  
  /**
   * Used for the data value representation of a Variable, presumably a Scalar.
   */
  type Value = String
  
  /**
   * Used for a Line of ASCII text from the data source.
   */
  type Line = String
  
  /**
   * A record may be one or more lines.
   */
  type Record = Seq[Line]
  
  
  lazy val variableNames = tsml.getVariableNames
  //TODO: generalize to get "projected" variables?
  
  /**
   * Keep track of the data Source so we can close it.
   */
  lazy private val source: Source = Source.fromURL(getUrl())
  
  /**
   * Get the character that is used at the start of a line to indicate that
   * it should not be read as data. Defaults to null, meaning that no line
   * should be ignored (except empty lines).
   */
  lazy private val commentCharacter: String = properties.get("commentCharacter") match {
    case Some(s) => s
    case None => null
  }
  
  /**
   * Return Iterator of lines, filter out lines deemed unworthy by "shouldSkipLine",
   * including empty lines (addresses problem at end of file) and lines starting 
   * with the "commentCharacter".
   * This is "lazy" so we won't access the data source until called upon.
   */
  lazy val lineIterator: Iterator[Line] = source.getLines().filterNot(shouldSkipLine(_))
  
  /**
   * This method will be used by the lineIterator to skip lines from the data source
   * that we don't want in the data. Adapters could override this to add special rules
   * but shouldn't bypass the existing ones. 
   * Note that the "isEmpty" test bypasses an end of file problem iterating over the 
   * iterator from Source.getLines.
   */
  def shouldSkipLine(line: Line): Boolean = {
    line.isEmpty() || (commentCharacter != null && line.startsWith(commentCharacter))
  }
  
  /**
   * This Iterator will return multiple lines of text for each record
   * as defined by the "linesPerRecord" attribute in the TSML.
   * Instead of merging all the lines into a single String, each sample
   * will be Seq of lines so the subclass can interpret it as it will.
   * This is "lazy" so we won't access the data source until called upon.
   */
  lazy val recordIterator: Iterator[Record] = lineIterator.grouped(linesPerRecord)
  //TODO: support iterating more than once?
  
  /**
   * The "linesPerRecord" attribute from this Adapter's definition in the TSML.
   * This value represents how many lines of text in the ASCII source are needed
   * for one record/sample of the outer Function.
   */
  val linesPerRecord: Int = properties.get("linesPerRecord") match {
    case Some(s) => s.toInt
    case None => 1
  }
  
  
  /**
   * Subclasses should implement this method to parse a "record" of text
   * into a Map of Variable name to value. This may be one or more lines
   * as defined by the "linesPerRecord" attribute of this adapter definition 
   * in the TSML.
   * Note, LinkedHashMap will maintain order.
   * Return empty Map if there was a problem with this record.
   * The metadata is for the Function Variable that we are iterating over.
   */
  //def parseRecord(metadata: FunctionMd, record: Record): LinkedHashMap[Name,ArrayBuffer[Value]]
  def parseRecord(record: Record): Map[Name, Value] = {
    //assume one line per record, space delimited
    (variableNames zip record(0).split(" ")).toMap
  }
  
  def makeIterableData(sampleTemplate: Sample): Data = new IterableData {
    def recordSize = sampleTemplate.size
    
    def iterator = new Iterator[Data] {
      override def hasNext = recordIterator.hasNext
      override def next = {
        val record = recordIterator.next
        val svals = parseRecord(record)
        makeDataFromRecord(sampleTemplate, svals)
      }
    }
  }
  
  def makeDataFromRecord(sampleTemplate: Sample, svals: Map[Name, Value]): Data = {
    //build a ByteBuffer
    val size = sampleTemplate.size
    val bb = ByteBuffer.allocate(size)
    
    //assume every Scalar in the template has a value in the Map, e.g. not stored by Tuple name
    //get Seq of Scalars from template
    val vars = sampleTemplate.toSeq 
    
    for (v <- vars) {
      val s = svals(v.name) //string value for the given scalar
      v match {
        //TODO: case time: Time => 
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
  
//  //suck in entire granule, for now
//  def readData: immutable.Map[String, immutable.Seq[String]] = {
//    val map = mutable.HashMap[Name, mutable.ArrayBuffer[Value]]()
//    for (vname <- variableNames) map += ((vname, mutable.ArrayBuffer[Value]()))
//        
//    while (recordIterator.hasNext) {
//      val record = recordIterator.next
//      val vs = parseRecord(record)
//      for (vname <- variableNames) map(vname) append vs(vname)
//    }
//    
//    //return as immutable dataMap
//    val z = for ((name, seq) <- map) yield name -> seq.toIndexedSeq //turn ArrayBuffer into an immutable Seq
//    z.toMap //turn HashMap into an immutable Map
//  }
  
  
  def close = source.close
}