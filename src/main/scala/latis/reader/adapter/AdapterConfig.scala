package latis.reader.adapter

import java.net.URL
import latis.util.StringUtils

case class AdapterConfig(properties: Map[String,String], processingInstructions: Seq[ProcessingInstruction]) {
    
  /**
   * Get the URL of the data source from this adapter's configuration.
   * This will come from the adapter's 'location' attribute.
   * It will try to resolve relative paths by looking in the classpath
   * then looking in the current working directory.
   */
  def getUrl: URL = {
    properties.get("location") match {
      case Some(loc) => StringUtils.getUrl(loc)
      case None => throw new RuntimeException("No 'location' property defined.")
    }
  }
}

case class ProcessingInstruction(name: String, args: String)

