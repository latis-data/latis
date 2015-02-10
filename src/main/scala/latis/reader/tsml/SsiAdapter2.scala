package latis.reader.tsml

import java.io.FileNotFoundException
import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel.MapMode

import scala.Option.option2Iterable

import latis.data.EmptyData
import latis.data.IterableData
import latis.data.SampleData
import latis.data.SampledData
import latis.data.seq.DataSeq
import latis.data.set.IndexSet
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.reader.tsml.ml.Tsml
import latis.util.BufferIterator
import latis.util.DataUtils
import latis.util.MappingIterator
import latis.util.PeekIterator

/**
 * Combines multiple binary files representing individual Variables into a single Dataset.
 */
class SsiAdapter2(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  override def makeScalar(s: Scalar): Option[Scalar] = {
    findData(s) match {
      case Some(data) => s match {
        case _: Real => Some(Real(s.getMetadata, data.iterator.next))
        case _: Integer => Some(Integer(s.getMetadata, data.iterator.next))
        case _: Text => Some(Text(s.getMetadata, data.iterator.next))
      }
      case None => None
    }
  }
  
  override def makeFunction(f: Function): Option[Function] = {
    val template = Sample(f.getDomain, f.getRange)
    findFunctionData(f) match {
      case Some(data) => Some(Function(template.domain, template.range, new MappingIterator[SampleData, Sample](data.iterator, sd => Some(DataUtils.dataToSample(sd, template))),f.getMetadata))
      case None => None
    }
  }
  
  private def findData(v: Variable): Option[IterableData] = v match {
    case scalar:   Scalar   => findScalarData(scalar)
    case tuple:    Tuple    => findTupleData(tuple)
    case function: Function => findFunctionData(function)
  }
  
  /**
   * Searches for a binary file with a name matching this Scalar's name. 
   * Reads the file in an iterative ByteBuffer and wraps in IterableData.
   */
  protected def findScalarData(s: Scalar): Option[IterableData] = try {
    val name = s.getName
    val location = getUrl.getPath + name + ".bin"
    val file = new RandomAccessFile(location, "r")
    val order = this.getProperty("byteOrder", "big-endian") match {
      case "big-endian" => ByteOrder.BIG_ENDIAN
      case "little-endian" => ByteOrder.LITTLE_ENDIAN
    }
    val bb = file.getChannel.map(MapMode.READ_ONLY, 0, file.length)
    bb.order(order)
    
    if(file.length == 0) None else Some(IterableData(new BufferIterator(bb, s), s.getSize))
  } catch {
    case fnfe: FileNotFoundException => s match {
      case _: Index => Some(IndexSet())
      case _ => None
    }
  }
    
  /**
   * Combines data from each Variable into data for a Tuple
   */
  private def findTupleData(tuple: Tuple): Option[IterableData] = {
    val data = tuple.getVariables.flatMap(findData(_)) 
    data.length match {
      case 0 => None
      case n => Some(data.reduceLeft(_.zip(_)))
    }
  }
  
  /**
   * Combines data for each Variable in a Function to make SampledData. 
   */
  private def findFunctionData(f: Function): Option[SampledData] = {
    val ddata = findData(f.getDomain).get
    val rdata = f.getRange match {
      case nf: Function => { //the domain of the inner function must be cached in order to be repeated in each sample of the outer function
        val len = nf.getMetadata("length").get.toInt //nested functions always have defined length
        val idata = findFunctionData(nf).getOrElse(EmptyData)
        
        val loopData = IterableData(new LoopIterator(idata.domainSet.iterator), idata.domainSet.recordSize)//makes an iterator that repeats the inner domain indefinitely.
        
        IterableData(SampledData(loopData,idata.rangeData).iterator.grouped(len).map(s => DataSeq(s)), idata.recordSize * len)
      }
      case e => findData(e).getOrElse(EmptyData)
    }
    val sdata = SampledData(ddata, rdata)
    if(sdata.isEmpty) None else Some(sdata)
  }
  
  def close = {}

  /**
   * Helper class to repeat the Domain of nested Functions
   */
  class LoopIterator[T >: Null](iterator: Iterator[T]) extends PeekIterator[T] {
    
    private var it = iterator.duplicate
    
    def getNext = {
      if(it._1 hasNext) it._1.next
      else {
        it = it._2.duplicate
        getNext
      }
    }
  }
  
}