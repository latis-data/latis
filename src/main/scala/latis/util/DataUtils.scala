package latis.util

import latis.data._
import latis.data.value.StringValue
import latis.dm._
import java.nio.ByteBuffer
import latis.time.Time
import latis.data.seq.SeqData
import latis.time.TimeFormat
import scala.collection.Map

object DataUtils {

  /**
   * Given a dataMap mapping Variable names to Data and a Variable template,
   * construct a Data object with data from the dataMap.
   */
  def makeDataFromDataMap(dataMap: Map[String, Data], variableTemplate: Variable, index: Int = -1): Data = {
    //build a ByteBuffer to contain the data
    val size = variableTemplate.getSize
    val bb = ByteBuffer.allocate(size)

    //Internal method to recursively populate the buffer
    def accumulateData(v: Variable) {
      //See if we have cached data for the given variable.
      //If not, keep iterating recursively.
      dataMap.get(v.getName) match {
        case Some(d) => {
          //TODO: consider trace debugging here
          val dbb = d.getByteBuffer
          val bytes = dbb.array
          bb.put(bytes)
        }
        case None => v match {
          case _: Index => bb.putInt(index)  //handle Index which should not have a value in the dataMap
          case Tuple(vars) => vars.map(accumulateData(_))
          case f: Function => f.iterator.map(accumulateData(_))
          case _: Scalar => throw new Error("No data found for " + v.getName)
        }
      }
    }

    //recursively populate the byte buffer
    accumulateData(variableTemplate)

    //TODO: test that we got the size right
    //rewind for use
    Data(bb.flip.asInstanceOf[ByteBuffer])
  }
  

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