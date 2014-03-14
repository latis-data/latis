package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import java.net.URL
import latis.dm.Dataset
import latis.ops.Operation
import latis.ops.Selection

/**
 * Use a regular expression to extract data values from a data record.
 */
class SparqlAdapter(tsml: Tsml) extends IterativeAsciiAdapter(tsml) {
  
  private var query: String = ""
  
  /**
   * Override to make sure the required 'query' selection has been made.
   */
  override def getDataset(ops: Seq[Operation]): Dataset = {
    query = findSelection(ops, "query") match {
      case Some(Selection(_,_,s)) => s
      case None => throw new RuntimeException("SparqlAdapter must have a 'query' selection.")
    }
    super.getDataset(ops)
  }
  
//  /**
//   * Get the required query string from the adapter definition.
//   */
//  lazy val query = getProperty("query") match {
//    case Some(s: String) => s
//    case None => throw new Error("The LemrAdapter requires a 'query' definition.")
//  }

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