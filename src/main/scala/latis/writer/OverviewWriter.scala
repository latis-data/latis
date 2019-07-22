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
import scala.collection.immutable.DefaultMap
import latis.reader.tsml.TsmlReader
import java.io.ByteArrayOutputStream
import java.io.FileNotFoundException
import latis.reader.CatalogReader
import latis.reader.FlatCatalogReader

class OverviewWriter(servletConfig: ServletConfig) {
  
  private val mainTmpl = StringTemplate.fromResource("templates/overviewwriter/main.html")
  private val suffixTmpl = StringTemplate.fromResource("templates/overviewwriter/suffix.html")
  private val operationTmpl = StringTemplate.fromResource("templates/overviewwriter/operation.html")
  
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
    val latisMapping = LatisProperties.getOrElse("latis.mapping", "dap")
    
    val values: Map[String, String] = Map(
        "context-root" -> s"$scheme://$host$contextRoot/$latisMapping",
        "dataset-name" -> catalog.getName,
        "catalog-html" -> catalogHtml(),
        "output-options-html" -> outputOptionsHtml(),
        "filter-options-html" -> filterOptionsHtml()
    )
    
    printWriter.print(mainTmpl.applyValues(values))
    printWriter.flush()
  }
  
  private lazy val catalog: Dataset = {
    try {
      val tsmlUrl = Catalog.getTsmlUrl("catalog")
      val reader = TsmlReader(tsmlUrl)
      reader.getDataset()
    } catch {
      case e: FileNotFoundException => CatalogReader().getDataset
    }
  }
  
  private def catalogHtml(): String = {
    val writer = new CatalogOverviewWriter
    val outputStream = new ByteArrayOutputStream
    writer.setOutputStream(outputStream)
    writer.write(catalog)
    
    outputStream.toString()
  }
  
  private def defaultStr(msg: String, defaultMsg: String): String = {
    if (msg != null) msg
    else defaultMsg
  }
  
  private def outputOptionsHtml(): String = {
    ServerMetadata.availableSuffixes.
      sortBy(suffixInfo => suffixInfo.suffix). // put in alphabetical order
      map(suffixInfo => // turn each info obj into Map and apply template 
        suffixTmpl.applyValues(Map(
          "suffix" -> suffixInfo.suffix,
          "className" -> suffixInfo.className,
          "description" -> defaultStr(
            suffixInfo.description,
            s"<span class='aside'>No Description: add <em>writer.${suffixInfo.suffix}.description</em> to latis.properties</span>"
          )
        ))
      ).
      mkString // simple string join and return
  }
  
  private def filterOptionsHtml(): String = {
    ServerMetadata.availableOperations.
      sortBy(opInfo => opInfo.name). // put in alphabetical order
      map(opInfo => operationTmpl.applyValues(Map( // turn each info obj into Map and apply template
          "name" -> opInfo.name,
          "className" -> opInfo.className,
          "description" -> defaultStr(
            opInfo.description,
            s"<span class='aside'>No Description: add <em>operation.${opInfo.name}.description</em> to latis.properties</span>"
          )
        ))
      ).
      mkString // simple
  }
}

object OverviewWriter {
  def apply(servletConfig: ServletConfig): OverviewWriter = new OverviewWriter(servletConfig)
}