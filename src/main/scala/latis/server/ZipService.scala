package latis.server

import java.io.PrintWriter
import java.io.ByteArrayInputStream
import java.net.URI
import java.net.URLEncoder
import com.typesafe.scalalogging.LazyLogging

import javax.servlet.http._
import latis.reader.JsonReader3
import latis.server.ErrorWriter
import latis.server.ZipService.validateRequest
import latis.util.LatisProperties
import latis.util.LatisServerProperties
import latis.writer.HttpServletWriter
import latis.writer.ZipWriter3
import play.api.libs.json.Json

import scala.io.Source

/**
 * Zip service with host validation for security.
 * Note that requests will fail to validate if they contain any URLs from hosts not whitelisted
 * by the zip.hosts.allowed property. If that property is not set then all requests will fail.
 */
class ZipService extends HttpServlet with LazyLogging {
  
  override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    //TODO: return error response
    //TODO: write instructions
    val msg = "The ZipService requires a POST."
    val writer = new PrintWriter(response.getOutputStream)
    writer.println(msg)
    writer.flush
  }
  
  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    //val mimeType = request.getContentType
    //assume json, for now
    
    /*
     * TODO: provide name for dataset and thus zip file?
     * 
     * will streaming start before reading all URLs?
     */
    
    logger.info(s"Processing request: zip")

    try {
      //Determine LaTiS service location and validate request
      val requestStr = Source.fromInputStream(request.getInputStream) //Note: storing request so it can be read twice
        .getLines
        .mkString(sys.props("line.separator"))

      validateRequest(requestStr)

      val ds = {
        val is = new ByteArrayInputStream(requestStr.getBytes())
        JsonReader3(is).getDataset()
      }

      //Note: needs ZipWriter3 to get content via URL
      HttpServletWriter(response, "zip3").write(ds)
    } catch {
      case e: Throwable => ErrorWriter(response).write(e)
    }

    logger.info("Request complete.")
  }
}

object ZipService {

  /**
   * Validate the given URL by returning whether its host has been whitelisted.
   */
  def validateUrl(url: String): Boolean = {
    //strip any whitespace and quotes, encode any query except for ampersands 
    val encodedUrl = if (url.contains('?')) {
      val split = url.trim.replaceAll("^\"|\"$", "").split('?') 
      split(0) + URLEncoder.encode(split(1), "UTF-8").replaceAll("%26", "&")
    } else url.trim.replaceAll("^\"|\"$", "")
    
    val host = new URI(encodedUrl).getAuthority
    val whiteList: Array[String] = LatisProperties.getOrElse("zip.hosts.allowed", "").split(",")
    whiteList.contains(host)
  }

  /**
   * Validate the given (stringified) HTTP servlet request by validating any URLs it contains.
   */
  def validateRequest(requestStr: String): Unit = {
    val urls: Seq[String] = {
      val json = Json.parse(requestStr)
      (json \\ "url").map(_.toString) //assumes the key for every URL value is "url"
    }
    urls.filter(!validateUrl(_)) match {
      case Seq() => //validated
      case Seq(url) =>
        val msg = s"ZipService cannot validate request with invalid URL: $url"
        throw new UnsupportedOperationException(msg)
      case us: Seq[String] =>
        val msg = s"ZipService cannot validate request with invalid URLs: ${us.mkString("\n")}"
        throw new UnsupportedOperationException(msg)
    }
  }
}
