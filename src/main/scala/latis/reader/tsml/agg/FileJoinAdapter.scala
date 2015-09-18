package latis.reader.tsml.agg

import scala.Option.option2Iterable
import latis.dm.Dataset
import latis.dm.Function
import latis.reader.tsml.TsmlAdapter
import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.Tsml
import latis.util.iterator.PeekIterator
import latis.dm.Sample
import latis.util.FileUtils
import latis.dm.Text
import java.io.File

/**
 * An AggregationAdapter that reads data from each file in a file list 
 * and aggregates the results. The tsml should be structured like so: 
 * 
 * <dataset>
 *   <adapter class="latis.reader.tsml.agg.FileJoinAdapter"/>
 *   
 *   <dataset>
 *     ...
 *     This dataset returns the list of files to be read
 *     ...
 *   </dataset>
 *   
 *   <dataset>
 *     ...
 *     A dataset that gives the structure of the final dataset.
 *     For each file name in the list above, the template will be 
 *     used to read a dataset and the results will be aggregated 
 *     using the TileUnionAdapter.
 *     ...
 *   </dataset>
 *   
 * </dataset>
 *     
 */
class FileJoinAdapter(tsml: Tsml) extends TileUnionAdapter(tsml) {
  
  /**
   * Construct the adapter for the file list dataset.
   */
  override protected val adapters = List(TsmlAdapter(Tsml((tsml.xml \ "dataset").head)))
  
  /**
   * Tsml for the template to be used to read each file.
   */
  val template = Tsml((tsml.xml \ "dataset").last)
  
  /**
   * Extract the file paths from a file list Dataset.
   */
  def getFilePaths(ds: Dataset): Iterator[String] = {
    lazy val dir = ds.getMetadata.get("srcDir") match {
      case None => ""
      case Some(sd) => sd + File.separator
    }
    
    ds match {
      case Dataset(Function(it)) => it.map(_.findVariableByName("file") match {
        case Some(Text(file)) => dir + file
        case None => throw new Exception(s"No 'file' Variable found in Dataset '$ds'")
      })
    }
  }
  
  /**
   * Read each file and aggregate the results.
   */
  override def collect(datasets: Seq[Dataset]): Dataset = {
    //Get list of file names from the first dataset.
    val files = getFilePaths(datasets.head) 
    
    //Make a TsmlReader for each file from the tsml template with the file location inserted.
    val readers = files.map(file => TsmlReader(template.setLocation(file)))
    
    //Make an iterator over each file dataset, appending their samples
    val sit = readers.flatMap(r => r.getDataset match {
      case Dataset(Function(it)) => new PeekIterator[Sample] {
        def getNext = it.next match {
          case null => r.close; null; //TODO: need better assurance that readers get closed
          case sample => sample
        }
      }
    })
    
    val f = TsmlAdapter(template).getOrigDataset match {
      case Dataset(f: Function) => f //get the function template
    }
    
    val md = makeMetadata(tsml.dataset)
    Dataset(Function(f, sit), md)
  }

}
