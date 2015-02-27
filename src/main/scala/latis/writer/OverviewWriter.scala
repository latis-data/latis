package latis.writer

import latis.dm.Dataset
import javax.servlet.http.HttpServletResponse
import latis.util.LatisProperties
import java.io.OutputStream
import java.io.PrintWriter
import javax.servlet.ServletConfig
import javax.servlet.http.HttpServletRequest
import latis.metadata.ServerMetadata
import latis.metadata.WriterDescription
import latis.metadata.Catalog

class OverviewWriter(servletConfig: ServletConfig) {
  
  private val mainTmpl = StringTemplate.fromResource("templates/overviewwriter/main.html")
  private val suffixTmpl = StringTemplate.fromResource("templates/overviewwriter/suffix.html")
  
  def write(httpRequest: HttpServletRequest, httpResponse: HttpServletResponse): Unit = {
    
    httpResponse.setContentType("text/html")
    val printWriter: PrintWriter = new PrintWriter(httpResponse.getOutputStream)
    
    val scheme = httpRequest.getScheme
    val port = httpRequest.getServerPort
    val isDefaultPort =
      (scheme == "https" && port == 443) ||
      (scheme == "http" && port == 80)
    val host =
      if (isDefaultPort) { httpRequest.getServerName }
      else { httpRequest.getServerName + ":" + port }
    val contextRoot = servletConfig.getServletContext.getContextPath
    
    val values: Map[String, String] = Map(
        "context-root" -> s"$scheme://$host$contextRoot/latis",
        "mission" -> LatisProperties.getOrElse("overview.mission", "MISSING: overview.mission (latis.properties)"),
        "catalog-html" -> catalogHtml(),
        "output-options-html" -> outputOptionsHtml(),
        "filter-options-html" -> filterOptionsHtml()
    )
    
    printWriter.print(mainTmpl.applyValues(values))
    printWriter.flush()
  }
  
  def catalogHtml(): String = {
    Catalog.listTsmlFiles().map(filename => s"<p>$filename</p>").mkString
  }
  
  def outputOptionsHtml(): String = {
    ServerMetadata.availableSuffixes.
      sortBy(suffixInfo => suffixInfo.suffix). // put in alphabetical order
      map(x => suffixTmpl.applyValues(x.toMap())). // apply each description object to the template
      mkString("") // simple string join and return
  }
  
  def filterOptionsHtml(): String = {
    "TODO: filter-options-html"
  }
}

object OverviewWriter {
  def apply(servletConfig: ServletConfig): OverviewWriter = new OverviewWriter(servletConfig)
}