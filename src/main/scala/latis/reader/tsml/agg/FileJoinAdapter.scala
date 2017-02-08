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
import latis.ops.Operation
import latis.ops.filter.Filter
import latis.ops.filter.Selection
import scala.collection.mutable.ArrayBuffer
import latis.metadata.Metadata

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
  //TODO: does this need to be a list? 
  
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
      case _ => Iterator.empty
    }
  }
  
  /**
   * Read each file and aggregate the results.
   */
  override def collect(datasets: Seq[Dataset]): Dataset = {
    //Get list of file names from the first dataset.
    val files = datasets.headOption match {
      case Some(ds) => getFilePaths(ds)
      case None => Iterator.empty
    }
    
    //Make a TsmlReader for each file from the tsml template with the file location inserted.
    val readers = files.map(file => TsmlReader(template.dataset.setLocation(file)))
    
    //Expose the Function's metadata, the last one should be fine
    var fmd: Metadata = Metadata.empty
    
    //Make an iterator over each file dataset, appending their samples
    val sit = readers.flatMap(r => r.getDataset match {
      case Dataset.empty => None
      case Dataset(f @ Function(it)) => new PeekIterator[Sample] {
        def getNext = it.next match {
          case null => r.close; null; //TODO: need better assurance that readers get closed
          case sample => fmd = f.getMetadata; sample
        }
      }
    }).buffered
    
    val md = makeMetadata(tsml.dataset)
    
    if(sit.hasNext) {
      val temp = sit.head  
      Dataset(Function(temp.domain, temp.range, sit, fmd), md)
    } else Dataset(null, md)
  }
  
  //operations to be passed to getDataset for each adapter (only file list but not file template?)
  lazy val toHandle = ArrayBuffer[Operation]()
  
  override def handleOperation(op: Operation): Boolean = op match {
    case s @ Selection(name, _, _) => {
      val ods = adapters.head.getOrigDataset
      ods.findVariableByName(name) match {
        case None => false //file list dataset does not have this parameter to select on //TODO: is this here because this used to cause selections to fail?
        case Some(_) => {
          toHandle += s
          val tods = TsmlAdapter(template).getOrigDataset //adapter for granule, not in global adapters list
          tods.findVariableByName(name) match {
            case None => true //if file template does not have this parameter then consider this operation handled;
              //it might be here only for the file list dataset
            case Some(_) => false //allow the operation to be handled elsewhere
          }
        }
      }
    }
    case f: Filter => {
      toHandle += f
      false
    }
    case _ => false
  }
  
  override def getDataset(ops: Seq[Operation]) = {
    val (handled, pass) = ops.partition(handleOperation)
    val dss = adapters.map(_.getDataset(toHandle))
    
    val ds = collect(dss)
    
    pass.foldLeft(ds)((dataset, op) => op(dataset)) 
  }

}
