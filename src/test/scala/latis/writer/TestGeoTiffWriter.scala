package latis.writer

import java.io.File

import org.junit.Test

import latis.reader.tsml.TsmlReader

class TestGeoTiffWriter {
  
  @Test
  def test {
    val file = new File("src/test/resources/datasets/test.tif")
    try{
      val ds = TsmlReader("src/test/resources/datasets/test/tiff.tsml").getDataset
      new GeoTiffWriter().writeFile(ds, file)
    }
    catch {
      case e: Exception => {
        file.delete
        throw e
      }
    }
    
  }
  
}