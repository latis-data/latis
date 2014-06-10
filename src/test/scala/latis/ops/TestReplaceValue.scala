package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.metadata.Metadata

class TestReplaceValue {

  @Test
  def test_real {
    val ds1 = Dataset(Real(Metadata("myReal"), 3.14))
    val ds2 = ds1.replaceValue(3.14, 6.28)

    assertEquals(3.14, ds1.toDoubles(0)(0), 0.0)
    assertEquals(6.28, ds2.toDoubles(0)(0), 0.0)
  }

  @Test
  def test_nan {
    val ds1 = Dataset(Real(Metadata("myReal"), Double.NaN))
    val ds2 = ds1.replaceValue(Double.NaN, 6.28)

    assertEquals(6.28, ds2.toDoubles(0)(0), 0.0)
  }
}




