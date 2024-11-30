package latis.ops

import latis.dm.Binary
import latis.dm.Dataset
import latis.dm.Scalar
import latis.metadata.Metadata
import org.junit.Assert._
import org.junit._

class TestUrlToBinary {

  @Test
  def image_url_to_binary(): Unit = {
    val md = Metadata("name" -> "url")
    val ds = Dataset(Scalar(md, "https://placecats.com/10/10"))
    new UrlToBinary()(ds) match {
      case Dataset(s @ Binary(bytes: Array[Byte])) =>
        assertEquals("image/jpeg", s.getMetadata("mediaType").get)
        assertEquals(349, bytes.length)
    }
  }

  @Test
  def override_media_type(): Unit = {
    val md = Metadata("name" -> "url", "mediaType" -> "text/plain")
    val ds = Dataset(Scalar(md, "https://placecats.com/10/10"))
    new UrlToBinary()(ds) match {
      case Dataset(b: Binary) =>
        assertEquals("text/plain", b.getMetadata("mediaType").get)
    }
  }


}
