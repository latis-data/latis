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
   * Override to make sure the required 'query' selection has been made.
   * Encode any special characters so they are suitable for a URL.
   */
  override def getDataset(ops: Seq[Operation]): Dataset = {
    //TODO: reconsider how/when PIs are applied
    //Note, this allows user to override what the tsml PI has
    query = findSelection(ops ++ piOps, "query") match {
      case Some(Selection(_,_,s)) => {
        //Note, still need to encode "#"s in prefix defs
        URLEncoder.encode(s, "UTF-8").replaceAll("#", "%23")
      }
      case None => throw new RuntimeException("SparqlAdapter must have a 'query' selection.")
    }
    super.getDataset(ops)
  }

  /**
   * Override to add query string to URL and request csv output.
   */
  override def getUrl(): URL = getProperty("location") match {
    case Some(s) => new URL(s + "?query=" + query + "&output=csv")
    case None => throw new Error("No 'location' found for LemrAdapter.")
  }

  /**
   * Override to skip first record (csv header).
   */
  override def getRecordIterator: Iterator[String] = {
    super.getRecordIterator.drop(1)
  }

}