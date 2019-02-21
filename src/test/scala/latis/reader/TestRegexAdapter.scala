package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._
import latis.dm._

class TestRegexAdapter extends AdapterTests {

  def datasetName = "regex"
  
  //@Test
  def test = writeDataset

  @Test
  def cols(): Unit =
    DatasetAccessor.fromName("regex_col").getDataset() match {
      case Dataset(Function(it)) => it.next match {
        case Sample(Text(t), TupleMatch(Text(c), Real(b), Integer(a))) =>
          assertEquals("1970/01/01", t)
          assertEquals("A", c)
          assertEquals(1.1, b, 0)
          assertEquals(1, a)
      }
    }
}
