package latis.reader

import latis.reader.tsml.ProtoBufAdapter
import org.junit.Test
import java.nio.ByteBuffer
import latis.reader.tsml.ml.Tsml
import latis.reader.tsml.TsmlReader
import latis.writer.TextWriter
import latis.writer.Writer

class TestProtoBufAdapter {
  
  //@Test
  def test_reader{
    val ds = TsmlReader("src/test/resources/datasets/test/proto.tsml").getDataset
    Writer.fromSuffix("asc").write(ds)
  }
  
  //@Test
  def test_parseVarint {
    val r = new ProtoBufAdapter(Tsml("src/test/resources/datasets/test/scalar.tsml"))
    val bb = ByteBuffer.allocate(2)
    bb.put(Array(0x11.toByte,0x02.toByte)).rewind
    println(r.parseKey(bb))
  }

}