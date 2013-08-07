package latis.data.seq

import scala.collection._
import java.nio.ByteBuffer
import latis.data.value.DoubleValue
import latis.data.Data

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
 *   
 * Use Array?
 *   ArrayData?
 *   better performance?
 *   but not immutable
 * 
 * TODO: mixed Data
 *   can't maintain seq of primitives
 *   seq of Any then convert on demand with pattern matching?
 *   or just make Seq of Data eagerly?
 */


//TODO: 2D Seq data for Tuple?
//inner, faster varying array is over Tuple elements
//assume doubles for now
//case class TupleSeqData(ts: Seq[Seq[Double]]) extends SeqData {
//  def doubleValue = ts(0)(0) //TODO: error?
//  def recordSize = 8 * ts(0).length
//  def length = ts.length
//  def iterator = ts.map(DoubleValue(_)).iterator
//}
