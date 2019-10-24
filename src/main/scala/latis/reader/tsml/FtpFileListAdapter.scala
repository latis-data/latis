package latis.reader.tsml

import java.net.URL
import org.apache.commons.net.ftp.FTPClient
import latis.reader.tsml.ml.Tsml

/**
 * Provides a dataset of files from an FTP server.
 *
 * This adapter requires a 'pattern' attribute in the TSML with a
 * regular expression specifying groups that correspond to variables
 * in the TSML. The last variable is assumed to be the URL to the file
 * and does not require a group in the regex.
 */
class FtpFileListAdapter(tsml: Tsml) extends RegexAdapter(tsml) {

  override def getRecordIterator: Iterator[String] =
    withFtpClient(getUrl) {
      _.listFiles(getUrl.getPath())
       .iterator
       .filter(f => f.isValid() && f.isFile())
       .map(_.getName())
    }

  // Extracts each group in the regex and appends the file URL.
  override def extractValues(name: String): List[String] =
    regex.findFirstMatchIn(name).map { m =>
      m.subgroups ++ List(getFullUrl(name))
    }.getOrElse(List.empty)

  // The FTP connection is only open within the block.
  private def withFtpClient[A](url: URL)(f: FTPClient => A): A = {
    val ftpClient = new FTPClient()
    val host = url.getHost()
    val port = if (url.getPort() == -1) {
      url.getDefaultPort()
    } else {
      url.getPort()
    }

    try {
      ftpClient.connect(host, port)
      ftpClient.enterLocalPassiveMode()

      val login = ftpClient.login("anonymous", "anonymous")
      if (!login) {
        throw new RuntimeException(s"Cannot login to FTP server: $host")
      }

      val res = f(ftpClient)

      ftpClient.logout()

      res
    } finally {
      ftpClient.disconnect()
    }
  }

  private def getFullUrl(filename: String): String =
    getUrl.toURI.resolve(filename).toString
}
