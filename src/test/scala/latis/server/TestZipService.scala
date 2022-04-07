package latis.server

import java.io.FileOutputStream
import org.junit._
import org.junit.Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.reader.JsonReader3
import latis.writer.ZipWriter3

import javax.servlet.http.HttpServletRequest
import scala.io.Source

class TestZipService {
  
  val json = """[{"url": "https://lasp.colorado.edu/lisird/latis/dap/tsis_tsi_24hr.txt?time>2019-01-01&take(10)"},
                 {"url": "https://lasp.colorado.edu/lisird/latis/dap/sorce_tsi_24hr_l3.asc?time>=2019-01-01&take(10)"},
                 {"url": "https://lasp.colorado.edu/lisird/latis/dap/tcte_tsi_24hr.json?time%3E2019-01-01&take(10)"}]"""

  val badJson = """[{"url": "http://lasp.colorado.edu/lisird/latis/dap/tsis_tsi_24hr.txt?time>2019-01-01&take(10)"},
                    {"url": "http://lasp.colorado.edu/sensitive_data.txt)"},
                    {"url": "https://my.sensitive.data.edu/everything.csv"}]"""
  
  //@Test
  def test = {
    val source = Source.fromString(json)
    val ds = JsonReader3(source).getDataset()
    //test baseUrl, not yet supported
//    val ds = Dataset(
//      Function(Seq(
//        Text(Metadata("url"), "tsis_tsi_24hr.txt?time>2019-01-01&take(10)"),
//        Text(Metadata("url"), "sorce_tsi_24hr_l3.asc?time>=2019-01-01&take(10)"),
//        Text(Metadata("url"), "tcte_tsi_24hr.json?time%3E2019-01-01&take(10)"),
//      )),
//      //Metadata("baseUrl" -> "https://lasp.colorado.edu/lisird/latis/dap/")
//    )
    
    //latis.writer.Writer.fromSuffix("asc").write(ds)
    val out = new FileOutputStream("/data/tmp/zipService.zip")
    val writer = ZipWriter3(out)
    writer.write(ds)

  }

  @Test
  def validateUrls: Unit = {
    val url1 = "http://lasp.colorado.edu/lisird/latis/dap/tsis_tsi_24hr.txt?time>2019-01-01&take(10)"
    val url2 = "http://lasp.colorado.edu/sensitive_data.txt"
    val url3 = "https://my.sensitive.data.edu/everything.csv"

    assertTrue(ZipService.validateUrl(url1))
    assertTrue(ZipService.validateUrl(url2))
    assertFalse(ZipService.validateUrl(url3))
  }

  @Test
  def validRequest: Unit = {
    ZipService.validateRequest(json)
  }

  @Test(expected=classOf[UnsupportedOperationException])
  def invalidRequest: Unit = {
    ZipService.validateRequest(badJson)
  }
}
