package latis.ops

import latis.dm.Sample

trait SampleHomomorphism {
  //def apply(sample: Sample)(implicit index: Int): Option[Sample] 
  def apply(sample: Sample): Option[Sample] 
}