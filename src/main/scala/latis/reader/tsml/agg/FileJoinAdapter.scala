package latis.reader.tsml.agg

import scala.Option.option2Iterable

import latis.dm.Dataset
import latis.dm.Function
import latis.reader.tsml.TsmlAdapter
import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.Tsml

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
 *   (More file list datasets may be included here to append to the 
 *   first file list.)
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
   * Get datasets to read the file list from. The last dataset is the template, so it is dropped.
   */
  override protected val adapters = (tsml.xml \ "dataset").map(n => TsmlAdapter(Tsml(n))).dropRight(1)
  
  val template = (tsml.xml \ "dataset").map(n => Tsml(n)).last
  
  /**
   * Read each file and aggregate the results.
   */
  override def collect(datasets: Seq[Dataset]): Dataset = {
    val z = datasets.zip(adapters.map(_.getUrl.toString.replaceAll(" ", "%20")))
    val files = z.flatMap(p => getFileName(p._1, p._2))
    
    val dss = files.map(file => TsmlReader(template.setLocation(file)).getDataset)
    super.collect(dss.toSeq)
  }
  
  /**
   * Get the full path of the 'file' variable in ds.
   */
  def getFileName(ds: Dataset, dir: String) = ds match {
    case Dataset(Function(it)) => it.flatMap(_.toSeq.find(_.hasName("file"))).map(dir + "/" +_.getValue.toString)
  }

}