package latis.data

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.DoubleValue

//abstract class SeqData(val seq: Seq[Data]) extends Data {
abstract class SeqData extends Data { //TODO: extends Seq[Data]?
  //def value = seq
//  override def iterator = seq.iterator
  //def isEmpty = length == 0
//  def length = seq.length
}

/*
 * 2013-08-07
 * TODO: still uncomfortable about a Scalar (type) having multiple Data values
 * implicitly equivalent to Function: Index -> Scalar?
 * the graph of the model is the model is the same, just hanging the data off a diff node
 * 
 * How can we emulate domain sets with various topologies?
 * it would need to BE-A Variable
 * need indexToValue and valueToIndex?
 * would that be generally useful for Data?
 *   take advantage of recordSize
 */

case class DoubleSeqData(ds: immutable.Seq[Double]) extends SeqData {
  
  def getByteBuffer: ByteBuffer = ds.foldLeft(ByteBuffer.allocate(size))(_.putDouble(_)).rewind.asInstanceOf[ByteBuffer]
  
  def length = ds.length
  def recordSize = 8
  
  def iterator = ds.iterator.map(DoubleValue(_))
  
  def apply(index: Int): Data = DoubleValue(ds(index))
}

//inner, faster varying array is over Tuple elements
//assume doubles for now
//case class TupleSeqData(ts: Seq[Seq[Double]]) extends SeqData {
//  def doubleValue = ts(0)(0) //TODO: error?
//  def recordSize = 8 * ts(0).length
//  def length = ts.length
//  def iterator = ts.map(DoubleValue(_)).iterator
//}


object SeqData {
  import scala.collection.Seq //Allow mutable inputs, convert to immutable for construction.
  
  def apply(ds: Seq[Double]): SeqData = DoubleSeqData(ds.toIndexedSeq)
  
//  def apply(ds1: Seq[Double], ds2: Seq[Double]): SeqData = {
//    TupleSeqData((ds1 zip ds2).map(p => Vector(p._1, p._2)).toIndexedSeq)
//  }
  
  //TODO: def apply(ds1: Seq[Double], ds2: Seq[Double]*): SeqData 
}