package latis.reader

import org.junit.Test
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.writer.Writer

class TestGeoTiffAdapter {
  
  def datasetName = "tiff"
  
  @Test
  def testread {
    val ds = TsmlReader("datasets/test/tiff.tsml").getDataset
    Writer.fromSuffix("txt").write(ds)
  }

}