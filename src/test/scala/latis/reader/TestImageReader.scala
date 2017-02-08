package latis.reader

import latis.reader.tsml.ml._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestImageReader  {
    
  //@Test 
  def image_2x2 {
    val ds = ImageReader("data/image_2x2.jpg").getDataset()
    latis.writer.AsciiWriter.write(ds)
    /*
     * ((row, col) -> (band0, band1, band2))
     * (0, 0) -> (0.0, 0.0, 0.0)
     * (0, 1) -> (254.0, 0.0, 0.0)
     * (1, 0) -> (0.0, 0.0, 254.0)
     * (1, 1) -> (0.0, 255.0, 0.0)
     * 
     * Image:
     *  Black  Red
     *  Blue   Green
     */
  }
  

}