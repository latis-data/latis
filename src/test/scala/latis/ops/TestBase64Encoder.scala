package latis.ops

import latis.data.Data
import latis.dm.Dataset
import latis.dm.Scalar
import latis.metadata.Metadata
import org.junit.Assert._
import org.junit._

class TestBase64Encoder {

  @Test
  def encode_text_with_media_type(): Unit = {
    val md = Metadata("name" -> "x", "mediaType" -> "text/plain")
    val ds = Dataset(Scalar("binary", md, Data("Test".getBytes())))
    new Base64Encoder()(ds) match {
      case Dataset(s @ Scalar(x: String)) =>
        assertEquals("text/plain;base64", s.getMetadata("mediaType").get)
        assertEquals("VGVzdA==", x)
    }
  }

  @Test
  def encode_text_without_media_type(): Unit = {
    val md = Metadata("name" -> "x")
    val ds = Dataset(Scalar("binary", md, Data("Test".getBytes())))
    new Base64Encoder()(ds) match {
      case Dataset(s: Scalar) =>
        assertTrue(s.getMetadata("mediaType").isEmpty)
    }
  }
}
