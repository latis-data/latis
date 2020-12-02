package latis.server

import java.io.FileOutputStream
import org.junit._

import latis.dm._
import latis.metadata.Metadata
import latis.writer.ZipWriter3

class TestZipService {
  
  val json = """[{"url": "http://lasp.colorado.edu/lisird/latis/dap/tsis_tsi_24hr.txt?time>2019-01-01&take(10)"},
                 {"url": "http://lasp.colorado.edu/lisird/latis/dap/sorce_tsi_24hr_l3.asc?time>2019-01-01&take(10)"},
                 {"url": "http://lasp.colorado.edu/lisird/latis/dap/tcte_tsi_24hr.json?time>2019-01-01&take(10)"}]"""
  
  //@Test
  def test = {
    //val source = Source.fromString(json)
    //val ds = JsonReader(source).getDataset()
    val ds = Dataset(
      Function(Seq(
        Text(Metadata("url"), "http://lasp.colorado.edu/lisird/latis/dap/tsis_tsi_24hr.txt?time>2019-01-01&take(10)"),
        Text(Metadata("url"), "http://lasp.colorado.edu/lisird/latis/dap/sorce_tsi_24hr_l3.asc?time%3E2019-01-01&take(10)"),
        Text(Metadata("url"), "http://lasp.colorado.edu/lisird/latis/dap/tcte_tsi_24hr.json?time%3E2019-01-01&take(10)"),
      )),
      //Metadata("baseUrl" -> "http://lasp.colorado.edu/lisird/latis/dap/")
    )
    
    //latis.writer.Writer.fromSuffix("asc").write(ds)
    val out = new FileOutputStream("/data/tmp/zipService.zip")
    val writer = ZipWriter3(out)
    writer.write(ds)

  }
}