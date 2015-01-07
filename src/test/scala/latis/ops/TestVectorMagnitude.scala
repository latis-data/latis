package latis.ops

import org.junit.Test

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestVectorMagnitude {
  
  //@Test
  def test {
    val samples = List(Sample(Real(Metadata("t"), 0), Tuple(Real(Metadata("a"), 0), Real(Metadata("b"), 0), Real(Metadata("c"), 0))), 
                       Sample(Real(Metadata("t"), 1), Tuple(Real(Metadata("a"), 1), Real(Metadata("b"), 1), Real(Metadata("c"), 1))), 
                       Sample(Real(Metadata("t"), 2), Tuple(Real(Metadata("a"), 2), Real(Metadata("b"), 2), Real(Metadata("c"), 1))))
    val ds = Dataset(Function(samples), Metadata("function_of_named_scalar"))
    AsciiWriter.write(new VectorMagnitudeDerivation(Seq("a","b","c","X"))(ds))
  }
  
  @Test
  def testtsml {
    AsciiWriter.write(TsmlReader("datasets/test/vecmag.tsml").getDataset)
  }

}