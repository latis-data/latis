package latis.util

import latis.data._
import latis.data.value.StringValue
import latis.dm._
import java.nio.ByteBuffer
import latis.time.Time
import latis.data.seq.SeqData

object Util {



  /*
   * TODO: The Data to Variable conversions make use of a ByteBuffer's
   * advancing position. We need to ensure that these are done atomically
   * to avoid side effects.
   * The data should contain all and only the data for the requested Variable(s).
   * 
   * How to deal with Index Function? Do we need to store index value?
   */

  def dataToSample(data: Data, template: Sample): Sample = {
    //TODO: could we just rely on Sample's Tuple behavior here?
    val bb = data.getByteBuffer
    //val sample = buildVarFromBuffer(bb, template)
    val domain = buildVarFromBuffer(bb, template.domain)
    val range = buildVarFromBuffer(bb, template.range)
    bb.rewind //reset to the beginning in case we want to reuse it
    Sample(domain, range)
  }

  def dataToVariable(data: Data, template: Variable): Variable = template match {
    case tup: Tuple => {
      //don't allow tuple to contain its own data, for now
      val bb = data.getByteBuffer
      val v = buildVarFromBuffer(bb, template)
      bb.rewind //reset to the beginning in case we want to reuse it
      v
    }
    //TODO: use builder
    case _: Time    => Time(template.getMetadata, data)
    case _: Real    => Real(template.getMetadata, data)
    case _: Integer => Integer(template.getMetadata, data)
    case _: Text    => Text(template.getMetadata, data)
    case _: Binary  => Binary(template.getMetadata, data)
    
    //TODO: deal with nested Function
    case f: Function => ???
  }
  
//  def dataToVariable(data: Data, template: Variable): Variable = {
//    //hack/experiment for text, don't use byte buffer, TODO: generalize?
//    data match {
//      case StringValue(s) => Text(template.getMetadata, s) //TODO: just build with Data instead of extracting string? or do we need to be able to reapply length?
//      case sd: SeqData => template match {
//        //TODO: make sure types match
//        //TODO: use builder
//        case _: Real => Real(template.getMetadata, data)
//        ???
//      }
////      case _ => {
////        //TODO: just build any var with given data? even Tuple?
////        val bb = data.getByteBuffer
////        val v = buildVarFromBuffer(bb, template)
////        bb.rewind //reset to the beginning in case we want to reuse it
////        v
////      }
//    }
//  }

  def buildVarFromBuffer(bb: ByteBuffer, template: Variable): Variable = template match {

    //TODO: use builder?
    
    case v: Time => v match {
      case _: Real => Time(template.getMetadata, bb.getDouble)
      case _: Integer => Time(template.getMetadata, bb.getLong)
      case t: Text => {
        val sb = new StringBuilder
        for (i <- 0 until t.length) sb append bb.getChar
        Time(template.getMetadata, sb.toString)
      }
    }
    
    case v: Index => Index(bb.getInt)
    case v: Real => Real(template.getMetadata, bb.getDouble)
    case v: Integer => Integer(template.getMetadata, bb.getLong)
    
    case v: Text => {
      val cs = new Array[Char](v.length)
      bb.asCharBuffer.get(cs)
      bb.position(bb.position + v.length * 2) //advance position in underlying buffer
      //val s = (0 until v.length).map(bb.getChar).mkString
      //TODO: why can't we just get chars from the bb?
      val s = new String(cs)
      Text(template.getMetadata, s)
    }
    
    case v: Binary => {
      val bytes = new Array[Byte](v.getSize)
      bb.get(bytes)
      val buffer = ByteBuffer.wrap(bytes)
      Binary(template.getMetadata, buffer)
    }

    //TODO: Don't include Index?
    //case Sample(_: Index, r: Variable) => Tuple(buildVarFromBuffer(bb, r), template.getMetadata)
      
    case Tuple(vars) => Tuple(vars.map(buildVarFromBuffer(bb, _)), template.getMetadata)

    case Function(d, r) => { //entire function, designed for inner functions
      //Function(buildVarFromData(bb, d), buildVarFromData(bb, r))
      //TODO: interleave
      //iterate over sample size

      ???
    }
  }
}