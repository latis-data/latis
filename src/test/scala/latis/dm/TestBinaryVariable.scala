package latis.dm

import org.junit._
import Assert._
import latis.data.value.DoubleValue
import latis.writer._
import java.io.FileOutputStream

class TestBinaryVariable {

  @Test
  def test {
    val b = Binary(DoubleValue(3.14).getByteBuffer)
    val r = Real(3.14)
    val ds = Dataset(r,b)
    val file = new FileOutputStream("/data/test.bin")
    BinaryWriter(file).write(ds)
    file.close
  }
}