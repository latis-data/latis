package latis.reader

import latis.reader.tsml.ProtoBufAdapter
import org.junit.Test
import org.junit.Assert._
import java.nio.ByteBuffer
import latis.reader.tsml.ml.Tsml
import latis.reader.tsml.TsmlReader
import latis.writer.TextWriter
import latis.writer.Writer
import java.io.FileOutputStream
import scala.io.Source

class TestProtoBufAdapter {
  
  var tmpFile = java.io.File.createTempFile("LaTiS", "WriterTest")
  tmpFile.deleteOnExit
  
  @Test
  def test_reader{
    val fos = new FileOutputStream(tmpFile)
    val ds = TsmlReader("datasets/test/proto.tsml").getDataset
    Writer(fos,"txt").write(ds)
    fos.close()
    val s = Source.fromFile(tmpFile).getLines
    val t = Source.fromFile(s"src/test/resources/datasets/data/tsi/txt").getLines
    while(t.hasNext) assertEquals(t.next, s.next)
    assert(s.isEmpty)
  }
  
  //@Test
  def test_parseVarint {
    val r = new ProtoBufAdapter(Tsml("src/test/resources/datasets/test/scalar.tsml"))
    val bb = ByteBuffer.allocate(2)
    bb.put(Array(0x11.toByte,0x02.toByte)).rewind
    println(r.parseKey(bb))
  }

}