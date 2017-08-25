package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import java.net.URL
import latis.dm.Dataset
import latis.ops.Operation
import latis.ops.filter.Selection
import java.net.URLEncoder

class SparqlAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {
  
  private var query: String = ""
  
  /**
   * Make sure the required 'query' tsml property has been defined.
   * Encode any special characters so they are suitable for a URL.
   */
  override def getDataset(ops: Seq[Operation]): Dataset = {
    query = getProperty("query") match { 
      case Some(q) => URLEncoder.encode(q, "UTF-8").replaceAll("#", "%23") //Encode "#"s in prefix defs
      case None => throw new RuntimeException("SparqlAdapter must have a 'query' tsml property defined.")
    }
    super.getDataset(ops)
  }

  /**
   * Override to add query string to URL and request csv output.
   */
  override def getUrl(): URL = getProperty("location") match {
    case Some(s) => new URL(s + "?query=" + query + "&output=csv")
    case None => throw new Error("No 'location' found for SparqlAdapter.")
  }

  /**
   * Override to skip first record (csv header).
   */
  override def getRecordIterator: Iterator[String] = {
    super.getRecordIterator.drop(1)
  }

}
