package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.dm._
import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel.MapMode
import latis.data.Data
import latis.data.buffer.ByteBufferData
import latis.data.SampledData
import latis.util.BufferIterator
import latis.data.IterableData
import latis.data.seq.DataSeq
import latis.util.MappingIterator
import latis.data.SampleData
import latis.util.DataUtils
import latis.util.PeekIterator

class SsiAdapter2(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  override def makeFunction(f: Function): Option[Function] = {
    val template = Sample(f.getDomain, f.getRange)
    makeSample(template) match {
      case None => None
      case Some(sample) => findFunctionData(f) match{
        case Some(data) => Some(Function(sample.domain, sample.range, new MappingIterator[SampleData, Sample](data.iterator, sd => Some(DataUtils.dataToSample(sd, sample))),f.getMetadata))
        case None => None
      }
    }
  }
  
  private def findData(v: Variable): Option[IterableData] = v match {
    case scalar:   Scalar   => findScalarData(scalar)
    case tuple:    Tuple    => findTupleData(tuple)
    case function: Function => findFunctionData(function)
  }
  
  protected def findScalarData(s: Scalar): Option[IterableData] = {
    val name = s.getName
    val location = getUrl.getPath + name + ".bin"
    val file = new RandomAccessFile(location, "r")
    val order = this.getProperty("byteOrder", "big-endian") match {
      case "big-endian" => ByteOrder.BIG_ENDIAN
      case "little-endian" => ByteOrder.LITTLE_ENDIAN
    }
    val bb = file.getChannel.map(MapMode.READ_ONLY, 0, file.length)
    bb.order(order)
    
    Some(IterableData(new BufferIterator(bb, s), s.getSize))
  }
    
  private def findTupleData(tuple: Tuple): Option[IterableData] = {
    val data = tuple.getVariables.flatMap(findData(_)) 
    data.length match {
      case 0 => None
      case n => Some(data.reduceLeft(_.zip(_)))
    }
  }
  
  private def findFunctionData(f: Function): Option[SampledData] = {
    val ddata = findData(f.getDomain).get
    val rdata = f.getRange match {
      case nf: Function => {
        val len = nf.getMetadata("length").get.toInt
        val sdata = findFunctionData(nf).get
        
        val loopData = IterableData(new LoopIterator(sdata.domainSet.iterator), sdata.domainSet.recordSize)
        
        IterableData(SampledData(loopData,sdata.rangeData).iterator.grouped(len).map(s => DataSeq(s)), sdata.recordSize * len)
      }
      case e => findData(e).get
    }
    val sdata = SampledData(ddata, rdata)
    Some(sdata)
  }
  
  def close = {}

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