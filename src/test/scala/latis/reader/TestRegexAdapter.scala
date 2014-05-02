package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._

class TestRegexAdapter extends AdapterTests {

  def datasetName = "regex"
  
  //@Test
  def test = writeDataset
}