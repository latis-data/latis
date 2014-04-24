package latis.ops

import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Index
import latis.dm.Variable

class IndexedSampleMappingOperation extends Operation {
  
  //set when applied to sample and used for setting value in Index Scalars
  private var _index = -1
  def getIndex = _index
  
  def apply(sample: Sample, index: Int): Option[Sample] = {
    _index = index
    applyToSample(sample) match {
      case Some(s: Sample) => Some(s)
      case _ => None 
    }
  }
    
  /**
   * Override to update Index values.
   */
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    super.applyToScalar(scalar) match {
      case Some(_: Index) => Some(Index(_index))
      case Some(s: Scalar) => Some(s)
      case _ => None 
    }
  }
}