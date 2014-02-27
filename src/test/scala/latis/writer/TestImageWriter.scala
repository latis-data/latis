package latis.writer

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import java.io.File
import java.io.FileOutputStream

class TestImageWriter {

  @Test
  def test{
    val file = new File("src/test/resources/datasets/data/tsi/plot.png")
    val fos = new FileOutputStream(file)
    val ds = TsmlReader("datasets/test/scalar2.tsml").getDataset
    Writer(fos, "image").write(ds)
  }

}