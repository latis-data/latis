package latis.reader

import java.net.URL
import scala.io.Source
import latis.ops._
import latis.dm._
import latis.metadata._

/**
 * Read entire contents of a matrix (2D) into a Dataset of type
 *   (row, column) -> value
 * The value will be of type Text.
 * row and column are 1-based Integer indexes, starting in the upper left.
 * Order is row-major (column index varying fastest).
 */
class AsciiMatrixReader(url: URL) extends DatasetSource {
  //TODO: make a Matrix subtype of SampledFunction with matrix semantics
  //TODO: allow value to be any Variable type?
  //TODO: impl as Adapter so we can hand it a model with metadata
  //TODO: optimize with index logic
  
  val delimiter = "," //TODO: parameterize
  
  private lazy val source = Source.fromURL(url)
  
  def getDataset(operations: Seq[Operation]): Dataset3 = {
    val rowmd = ScalarMetadata(Map("id" -> "row", "type" -> "integer"))
    val colmd = ScalarMetadata(Map("id" -> "column", "type" -> "integer"))
    val valmd = ScalarMetadata(Map("id" -> "value", "type" -> "text"))
    
    val rcmd = TupleMetadata(Seq(rowmd,colmd))(Map.empty)
    val fmd = FunctionMetadata(rcmd, valmd)(Map.empty)
    val dsmd = Metadata3(fmd)(Map("id" -> "matrix"))
    
    val data: Array[Array[String]] = source.getLines.map(_.split(delimiter)).toArray
    
    val nrow = data.length
    val ncol = data(0).length //TODO: assert that all rows have the same number of samples
    
    val samples: Seq[Sample3] = for (
      irow <- 1 to nrow;
      icol <- 1 to ncol
    ) yield {
      val row = Scalar3(irow.toLong)(rowmd)
      val col = Scalar3(icol.toLong)(colmd)
      val value = Scalar3(data(irow-1)(icol-1))(valmd)
      val domain = Tuple3(Seq(row,col))(Map.empty)
      Sample3(domain, value)
    }
    
    Dataset3(SampledFunction3(samples)(fmd))(dsmd)
    //TODO: apply operations
  }
  
  
  def close = source.close() //TODO: don't make lazy source just to close it
}

object AsciiMatrixReader {
  
  def apply(file: String): AsciiMatrixReader = {
    //TODO relative path, resource
    new AsciiMatrixReader(new URL(file))
  }
}