package latis.writer

import latis.data.value.DoubleValue
import latis.data.value.StringValue
import latis.dm._
import latis.metadata.Metadata
import latis.ops.UrlListToZipList
import latis.reader.tsml.TsmlReader
import latis.time._
import latis.util.DataUtils
import org.junit.Assert.assertEquals
import org.junit.Ignore
import org.junit.Test

import java.io.{File, FileOutputStream}
import java.util.zip.ZipFile


class TestZipWriter {
  
  @Test(expected=classOf[IllegalArgumentException])
  def non_file_list {
    val w = Writer("foo.zip")
    val ds = TsmlReader("log/log_join.tsml").getDataset
    w.write(ds)
  }
  
  @Test
  def zip_files {
    val w = Writer("/tmp/test.zip")
    val ds = TsmlReader("log/log_list.tsml").getDataset
    w.write(ds)
    // look through our zip file for big image files and clean up zipfile
    val tempFile = new File("/tmp/test.zip")
    val tempZipFile = new ZipFile(tempFile)
    val zipEntries = tempZipFile.entries
    var fileCount = 0
    while (zipEntries.hasMoreElements) {
      fileCount = fileCount + 1
      zipEntries.nextElement
    }
    tempFile.delete
    assertEquals(3, fileCount)
  }

  @Test @Ignore //TODO: ZipWriter can't handle urls and ZipWriter2 doesn't handle files (LATIS-476)
  def zip_urls {
    val samples = List(Sample(Integer(0), Text(Metadata("file"),"https://www.google.com/?gws_rd=ssl")), 
                       Sample(Integer(1), Text(Metadata("file"),"https://www.google.com/bogus")))
    val ds = Dataset(Function(samples), Metadata("function_of_urls"))
    //AsciiWriter.write(ds)
    val w = Writer("/tmp/test.zip")
    w.write(ds)
    
    assertEquals(3, 3)
  }

  @Test
  def zip_entry_from_url_ignoring_query(): Unit = {
    val url = "http://example.com/foo/bar.jar?who=dat"
    val name = new UrlListToZipList().makeNameUrlPair(url)._1
    assertEquals("bar.jar", name)
  }

  @Test
  def zip_entry_from_url_with_ending_slash(): Unit = {
    val url = "http://example.com/foo/bar/?who=dat"
    val name = new UrlListToZipList().makeNameUrlPair(url)._1
    assertEquals("bar", name)
  }

  @Test
  def disambiguate_duplicate_entry_names_url_int(): Unit = { //TODO: this would pass if we disambiguated with domain values but only because the lucky domain values 0,1,2; should change those to not "_n" (e.g. 1,2,3)
    //Note, only supported for ZipWriter3
    val samples = List(
      Sample(Integer(1), Text(Metadata("url"),"https://placekitten.com/200/200")),
      Sample(Integer(2), Text(Metadata("url"),"https://placekitten.com/200/200")),
      Sample(Integer(3), Text(Metadata("url"),"https://placekitten.com/200/200")),
    )
    val ds = Dataset(Function(samples), Metadata("test"))
    //AsciiWriter.write(ds)
    val file = new File("/tmp/disambiguate_duplicate_entry_names.zip")
    val fos = new FileOutputStream(file)
    ZipWriter3(fos).write(ds)

    import collection.JavaConverters._
    val entries = new ZipFile(file).entries.asScala.map(_.getName).toList
    assertEquals(List("200", "200_2", "200_3"), entries)
    file.delete
  }

  @Test
  def disambiguate_duplicate_entry_names_binary_text_time(): Unit = {
    //Note, only supported for ZipWriter3
    val samples = List(
      Sample(
        Time("text", Metadata(Map("units" -> "yyyy-MM-dd")), StringValue("2001-01-01")), 
        Tuple(List(Text(Metadata("url"),"https://placekitten.com/200/200"), binary_string, binary_double))
      ),
      Sample(
        Time("text", Metadata(Map("units" -> "yyyy-MM-dd")), StringValue("2001-01-02")), 
        Tuple(List(Text(Metadata("url"),"https://placekitten.com/200/200"), binary_string, binary_double))
      ),
      Sample(
        Time("text", Metadata(Map("units" -> "yyyy-MM-dd")), StringValue("2001-01-03")), 
        Tuple(List(Text(Metadata("url"),"https://placekitten.com/200/200"), binary_string, binary_double))
      )
    )
    val ds = Dataset(Function(samples), Metadata("test"))
    //AsciiWriter.write(ds)
    val file = new File("/tmp/disambiguate_duplicate_entry_names.zip")
    val fos = new FileOutputStream(file)
    ZipWriter3(fos).write(ds)

    import collection.JavaConverters._
    val entries = new ZipFile(file).entries.asScala.map(_.getName).toList
    assertEquals(List("test", "test_2001-01-02", "test_2001-01-03"), entries)
    file.delete
  }
  //TODO: disambiguation of Binary dataset (text time domain, number time domain, tuple domain) (Integer is already tested above, but Text and Real aren't!)
  //--------------------------------------------------------------------------------------------------------------------

  def binary_double = Binary(Metadata("binary_dub"), DataUtils.terminateBytes(DoubleValue(3.14).getByteBuffer))
  def binary_string = Binary(Metadata("binary_str"), DataUtils.terminateBytes(StringValue("Hello").getByteBuffer))
  
}
