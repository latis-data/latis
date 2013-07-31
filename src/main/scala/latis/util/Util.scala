package latis.util

import latis.data._
import latis.dm._
import java.nio.ByteBuffer
import latis.time.Time

object Util {

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
    val range  = buildVarFromBuffer(bb, template.range)
    bb.rewind //reset to the beginning in case we want to reuse it
    Sample(domain, range)
  }
  
  def dataToVariable(data: Data, template: Variable): Variable = {
    val bb = data.getByteBuffer
    val v = buildVarFromBuffer(bb, template)
    bb.rewind //reset to the beginning in case we want to reuse it
    v
  }
  
  def buildVarFromBuffer(bb: ByteBuffer, template: Variable): Variable = template match {
    
    case v: Time => Time(template.metadata, bb.getDouble)
    case v: Real => Real(template.metadata, bb.getDouble)
    //TODO: use builder?
    
    //TODO: other types
    
    case Tuple(vars) => Tuple(vars.map(buildVarFromBuffer(bb, _))) //TODO:, template.metadata)
    
    case Function(d, r) => { //entire function, designed for inner functions
      //Function(buildVarFromData(bb, d), buildVarFromData(bb, r))
      //TODO: interleave
      //iterate over sample size
      
      ???
    }
  }
}