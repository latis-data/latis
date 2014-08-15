package latis.writer

import org.junit.Test
import latis.reader.tsml.TsmlReader
import latis.util.FileUtils
import java.io.File

class TestGeoTiffWriter {
  
  @Test
  def test {
    val file = new File("src/test/resources/datasets/test.tif")
    try{
      val ds = TsmlReader("src/test/resources/datasets/test/tiff.tsml").getDataset
      //AsciiWriter.write(ds)
      new GeoTiffWriter().writeFile(ds, file)
      //file.delete
    }
    catch {
      case e: Exception => {
        file.delete
        throw e
      }
    }
    
  }

}