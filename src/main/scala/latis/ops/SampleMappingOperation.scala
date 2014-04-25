package latis.ops

import latis.dm.Sample

class SampleMappingOperation extends Operation {

  def apply(sample: Sample): Option[Sample] = {
    applyToSample(sample) match {
      case Some(s: Sample) => Some(s)
      case _ => None //TODO: error?
    }
  }
}