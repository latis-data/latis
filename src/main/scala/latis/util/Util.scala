package latis.util

import latis.data._
import latis.data.value.StringValue
import latis.dm._
import java.nio.ByteBuffer
import latis.time.Time

object Util {

  /**
   * Resolve any property references in the given string, 
   * e.g. ${dataset.dir}
   */
  def resolveParameterizedString(s: String): String = {
    //val pattern = """${(.+)}""".r //TODO: restrict to \w\\.
    //pattern.replaceAllIn(s, (m: Match) => LatisProperties(m.matched))
    //TODO: support more than one parameter?
    s.indexOf("${") match {
      case -1 => s
      case i1: Int => s.indexOf('}', i1) match {
        case -1 => s
        case i2: Int => {
          val prop = s.substring(i1 + 2, i2)
          LatisProperties.get(prop) match {
            case Some(p) => s.substring(0,i1) + p + s.substring(i2 + 1)
            case None => s //TODO: error, property not found? but would disallow legit values of that form
          }
        }
      }
    }
  }

  /*
   * TODO: The Data to Variable conversions make use of a ByteBuffer's
   * advancing position. We need to ensure that these are done atomically
   * to avoid side effects.
   * The data should contain all and only the data for the requested Variable(s).
   */

  def dataToSample(data: Data, template: Sample): Sample = {
    //TODO: could we just rely on Sample's Tuple behavior here?
    val bb = data.getByteBuffer
    val domain = buildVarFromBuffer(bb, template.domain)
    val range = buildVarFromBuffer(bb, template.range)
    bb.rewind //reset to the beginning in case we want to reuse it
    Sample(domain, range)
  }

  def dataToVariable(data: Data, template: Variable): Variable = {
    //hack/experiment for text, don't use byte buffer, TODO: generalize?
    data match {
      case StringValue(s) => Text(template.getMetadata, s)
      case _ => {
        val bb = data.getByteBuffer
        val v = buildVarFromBuffer(bb, template)
        bb.rewind //reset to the beginning in case we want to reuse it
        v
      }
    }
  }

  def buildVarFromBuffer(bb: ByteBuffer, template: Variable): Variable = template match {

    //TODO: use builder?
    
    case v: Index => Index(bb.getInt)
    //case v: Time => Time(template.getMetadata, bb.getDouble)
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

    case Tuple(vars) => Tuple(vars.map(buildVarFromBuffer(bb, _)), template.getMetadata)

    case Function(d, r) => { //entire function, designed for inner functions
      //Function(buildVarFromData(bb, d), buildVarFromData(bb, r))
      //TODO: interleave
      //iterate over sample size

      ???
    }
  }
}