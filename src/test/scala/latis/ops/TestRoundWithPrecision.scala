package latis.ops

import org.junit._
import Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.ops.RoundWithPrecision

class TestRoundWithPrecision {

  @Test
  def test_round {
    val testMetadata: Metadata = Metadata(("name", "t"), ("precision", "2"))

    val testScalar: Scalar = Scalar(testMetadata, 42.1234)

    val testSamples = List(testScalar)

    
    
  }
}
