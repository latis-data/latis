package latis.reader

import java.io.File
import java.io.FileOutputStream

import org.junit.Test

import latis.reader.tsml.TsmlReader
import latis.writer.Writer

class TestGeoTiffAdapter {
  
  @Test
  def testread {
    val ds = TsmlReader("datasets/test/tiff.tsml").getDataset
    val f = new File("src/test/resources/datasets/test.txt")
    val w = Writer(new FileOutputStream(f), "txt").write(ds)
  }

}