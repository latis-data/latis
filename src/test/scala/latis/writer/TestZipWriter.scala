package latis.writer

import org.junit.{Test,Ignore}
import latis.reader.tsml.TsmlReader
import org.junit.rules.TemporaryFolder
import java.io.File
import org.junit.Assert.assertEquals
import java.util.zip.ZipFile
import latis.dm.Sample
import latis.dm.Integer
import latis.dm.Text
import latis.dm.Dataset
import latis.dm.Function
import latis.metadata.Metadata


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
  
}