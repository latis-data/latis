package latis.writer

import collection.JavaConverters._

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

import java.io.File
import java.io.FileOutputStream
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

  def binary_string = Binary(Metadata("binary_str"), DataUtils.terminateBytes(StringValue("Hello").getByteBuffer))

  @Test
  def disambiguate_duplicate_entry_names_int_url(): Unit = {
    //Note, only supported for ZipWriter3
    val samples = List(
      Sample(Integer(1), Text(Metadata("url"),"https://placehold.co/200x200.jpeg")),
      Sample(Integer(2), Text(Metadata("url"),"https://placehold.co/200x200.jpeg")),
      Sample(Integer(3), Text(Metadata("url"),"https://placehold.co/200x200.jpeg")),
    )
    val ds = Dataset(Function(samples), Metadata("test"))
    val file = new File("/tmp/disambiguate_duplicate_entry_names_1.zip")
    val fos = new FileOutputStream(file)
    ZipWriter3(fos).write(ds)

    val entries = new ZipFile(file).entries.asScala.map(_.getName).toList
    assertEquals(List("200x200.jpeg", "200x200.jpeg_1", "200x200.jpeg_2"), entries)
    file.delete
  }

  @Test
  def disambiguate_duplicate_entry_names_int_binary(): Unit = {
    //Note, only supported for ZipWriter3
    val samples = List(
      Sample(Integer(1), binary_string),
      Sample(Integer(2), binary_string),
      Sample(Integer(3), binary_string),
    )
    val ds = Dataset(Function(samples), Metadata("test"))
    val file = new File("/tmp/disambiguate_duplicate_entry_names_2.zip")
    val fos = new FileOutputStream(file)
    ZipWriter3(fos).write(ds)

    val entries = new ZipFile(file).entries.asScala.map(_.getName).toList
    assertEquals(List("test_1", "test_2", "test_3"), entries)
    file.delete
  }

  @Test
  def disambiguate_duplicate_entry_names_tuple_url(): Unit = {
    //Note, only supported for ZipWriter3
    val samples = List(
      Sample(
        Tuple(List(Text("a"), Real(1.1))),
        Text(Metadata("url"),"https://placehold.co/200x200.jpeg")
      ),
      Sample(
        Tuple(List(Text("b"), Real(2.2))),
        Text(Metadata("url"),"https://placehold.co/200x200.jpeg")
      ),
      Sample(
        Tuple(List(Text("c"), Real(3.3))),
        Text(Metadata("url"),"https://placehold.co/200x200.jpeg")
      )
    )
    val ds = Dataset(Function(samples), Metadata("test"))
    val file = new File("/tmp/disambiguate_duplicate_entry_names_3.zip")
    val fos = new FileOutputStream(file)
    ZipWriter3(fos).write(ds)

    val entries = new ZipFile(file).entries.asScala.map(_.getName).toList
    assertEquals(List("200x200.jpeg", "200x200.jpeg_1", "200x200.jpeg_2"), entries)
    file.delete
  }

  @Test
  def disambiguate_duplicate_entry_names_tuple_binary(): Unit = {
    //Note, only supported for ZipWriter3
    val samples = List(
      Sample(Tuple(List(Text("a"), Real(1.1))), binary_string),
      Sample(Tuple(List(Text("b"), Real(2.2))), binary_string),
      Sample(Tuple(List(Text("c"), Real(3.3))), binary_string)
    )
    val ds = Dataset(Function(samples), Metadata("test"))
    val file = new File("/tmp/disambiguate_duplicate_entry_names_4.zip")
    val fos = new FileOutputStream(file)
    ZipWriter3(fos).write(ds)

    val entries = new ZipFile(file).entries.asScala.map(_.getName).toList
    assertEquals(List("test_a_1.1", "test_b_2.2", "test_c_3.3"), entries)
    file.delete
  }

  @Test
  def disambiguate_duplicate_entry_names_text_time_binary(): Unit = {
    //Note, only supported for ZipWriter3
    //Note, the Binary variable gets found first so "url" is ignored
    val samples = List(
      Sample(
        Time("text", Metadata(Map("units" -> "yyyy-MM-dd")), StringValue("2001-01-01")),
        Tuple(List(Text(Metadata("url"),"https://placehold.co/200x200.jpeg"), binary_string))
      ),
      Sample(
        Time("text", Metadata(Map("units" -> "yyyy-MM-dd")), StringValue("2001-01-02")),
        Tuple(List(Text(Metadata("url"),"https://placehold.co/200x200.jpeg"), binary_string))
      ),
      Sample(
        Time("text", Metadata(Map("units" -> "yyyy-MM-dd")), StringValue("2001-01-03")),
        Tuple(List(Text(Metadata("url"),"https://placehold.co/200x200.jpeg"), binary_string))
      )
    )
    val ds = Dataset(Function(samples), Metadata("test"))
    val file = new File("/tmp/disambiguate_duplicate_entry_names_5.zip")
    val fos = new FileOutputStream(file)
    ZipWriter3(fos).write(ds)

    val entries = new ZipFile(file).entries.asScala.map(_.getName).toList
    assertEquals(List("test_2001-01-01", "test_2001-01-02", "test_2001-01-03"), entries)
    file.delete
  }

  @Test
  def disambiguate_duplicate_entry_names_number_time_binary(): Unit = {
    //Note, only supported for ZipWriter3
    val samples = List(
      Sample(
        Time("real", Metadata(Map("units" -> "years since 2000-01-01")), DoubleValue(1.5)),
        binary_string
      ),
      Sample(
        Time("real", Metadata(Map("units" -> "years since 2000-01-01")), DoubleValue(2)),
        binary_string
      ),
      Sample(
        Time("real", Metadata(Map("units" -> "years since 2000-01-01")), DoubleValue(2.5)),
        binary_string
      )
    )
    val ds = Dataset(Function(samples), Metadata("test"))
    val file = new File("/tmp/disambiguate_duplicate_entry_names_6.zip")
    val fos = new FileOutputStream(file)
    ZipWriter3(fos).write(ds)

    val entries = new ZipFile(file).entries.asScala.map(_.getName).toList
    assertEquals(List("test_2001-07-01T12:00:00.000", "test_2001-12-31T00:00:00.000", "test_2002-07-01T12:00:00.000"), entries)
    file.delete
  }
}
