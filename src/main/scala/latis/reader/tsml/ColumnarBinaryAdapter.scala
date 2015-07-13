package latis.reader.tsml

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel.MapMode
import scala.Array.canBuildFrom
import scala.Option.option2Iterable
import scala.collection.mutable.ListBuffer
import latis.data.Data
import latis.data.value.DoubleValue
import latis.data.value.LongValue
import latis.data.value.StringValue
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text
import latis.dm.Variable
import latis.reader.tsml.ml.Tsml
import latis.util.iterator.BufferIterator
import latis.util.iterator.PeekIterator
import latis.util.StringUtils
import latis.util.iterator.LoopIterator
import latis.util.iterator.RepeatIterator
import latis.util.iterator.ZipIterator

/**
 * Combines separate binary files representing individual Variables into a single Dataset.
 */
class ColumnarBinaryAdapter(tsml: Tsml) extends IterativeAdapter2[Seq[Array[Byte]]](tsml) {
  
  lazy val order = getProperty("byteOrder", "big-endian") match {
    case "big-endian" => ByteOrder.BIG_ENDIAN
    case "little-endian" => ByteOrder.LITTLE_ENDIAN
  }
  
  /**
   * Gets the ByteBuffer for each Scalar in the Dataset from the file
   * with that Scalar's name in the directory specified in the tsml.
   */
  def getBuffers: Seq[ByteBuffer] = {
    val names = getOrigScalarNames
    val locs = names.map(n => new File(getUrlFile, n + ".bin"))
    val files = locs.map(loc => new RandomAccessFile(loc, "r"))
    
    val channels = files.map(_.getChannel)
    val bbs = channels.map(channel => channel.map(MapMode.READ_ONLY, 0, channel.size))
    
    //bbs will remain valid even after closing resources
    channels.foreach(_.close)
    files.foreach(_.close)
    
    
    bbs.foreach(_.order(order))
    bbs
  }
  
  /**
   * A list of Functions in the Dataset. Assumes Functions are nested linearly;
   * i.e. there are no Tuples with more than one Function.
   */
  val functions: List[Function] = { 
    if(getOrigDataset.unwrap.findFunction.isEmpty) List()
    val b = ListBuffer[Function]()
    b += getOrigDataset.unwrap.findFunction.get
    while(b.last.getRange.findFunction.nonEmpty) b += b.last.getRange.findFunction.get
    b.toList
  }

  /**
   * Wraps each Scalar's ByteBuffer in an Iterator and then repeats values or loops 
   * that Iterator as necessary. 
   */
  def getRecordIterator: Iterator[Seq[Array[Byte]]] = {
    val buffers = getBuffers
    val vars = getOrigScalars
    
    //map each domain to how many times its values must be repeated 
    //(the product of the length of its inner functions)
    //this is how many rows one Sample of that Function would take in a table format
    val reps = functions.map(_.getDomain).zip(functions.map(f => innerLength(f.getSample))).toMap
    
    //inner Function domains must be looped because they are repeated for each outer domain Sample
    val loops = functions.map(_.getRange.findFunction).flatten.map(_.getDomain)
    
    val its = buffers.zip(vars).map(p => BufferIterator(p._1, p._2))
    val rits = its.zip(vars).map(p => new RepeatIterator(p._1, reps.withDefaultValue(1)(p._2)))//apply repetitions
    val lrits = rits.zip(vars).map(p => if(loops.contains(p._2)) new LoopIterator(p._1) else p._1)//apply loops
    new ZipIterator(lrits)//ZipIterator changes Seq[Iterator[Array[Byte]]] => Iterator[Seq[Array[Byte]]]
  }
  
  def parseRecord(rec: Seq[Array[Byte]]): Option[Map[String,Data]] = {
    val vars = getOrigScalars
    Some(vars.zip(rec).map(p => (p._1.getName, makeData(p._2, p._1))).toMap)
  }
  
  /**
   * Make Data for the correct Scalar type given a template and Byte Array 
   */
  def makeData(bytes: Array[Byte], vTemplate: Variable): Data = vTemplate match {
    case _: Integer => LongValue(ByteBuffer.wrap(bytes).order(order).getLong)
    case _: Real => DoubleValue(ByteBuffer.wrap(bytes).order(order).getDouble)
    case t: Text => {
      val buffer = ByteBuffer.wrap(bytes).order(order).asCharBuffer
      val chars = Array.ofDim[Char](t.length)
      buffer.get(chars)
      StringValue(StringUtils.padOrTruncate(new String(chars),t.length))
    }
  }
  
  def close = {}
  
}
