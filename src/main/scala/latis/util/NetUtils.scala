package latis.util

import java.io.InputStream
import java.net.URL

object NetUtils {

  /**
   * Returns an Array of Bytes from a URL and an optional content type.
   */
  def readUrl(url: String): (Array[Byte], Option[String]) = {
    if (url.startsWith("http")) {
      val r = requests.get(url)
      (r.bytes, r.httpContentType)
    } else {
      var is: InputStream = null
      var bytes = Array[Byte]()
      try {
        is = new URL(url).openStream()
        bytes = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
      } finally {
        if (is != null) is.close()
      }
      (bytes, None)
    }
  }
}
