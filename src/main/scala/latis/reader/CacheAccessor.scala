package latis.reader

import latis.ops.Operation
import latis.dm._
import latis.util.CacheManager
import latis.metadata.Metadata
import latis.time.Time

/**
 * A DatasetAccessor to expose cached Dataset information
 * as a Dataset.
 */
class CacheAccessor extends DatasetAccessor {
  
  /**
   * Construct a Dataset from the CacheManager.
   *   time -> (name, size)
   * Where
   *   time: is a Time with native Java time units (ms since 1970).
   *   name: is the dataset name as defined in the Dataset metadata.
   *   size: is the size of the Data in bytes if it were serialized to a ByteArray
   *     thus the full text buffer is counted. The actual memory usage will be a 
   *     bit higher.
   */
  def getDataset(operations: Seq[Operation]): Dataset = {
    //Create a sample for each dataset in the cache
    val samples = CacheManager.getDatasets.map(p => {
      //create the 'name' variable
      val name = Text(Metadata("name"), p._1)
      
      //create the 'time' variable from the creation_time metadata
      val ds = p._2
      val time = ds.getMetadata.get("creation_time") match {
        case Some(ct) => Time(ct.toLong)
        case None => Time(0) //TODO: error if creation time not defined?
      }
      
      //create the 'size' variable based on the size of the Data in the Dataset
      val sizemd = Metadata(Map("name" -> "size", "units" -> "bytes"))
      val size = Integer(sizemd, ds.getSize)
      
      //construct the sample
      Sample(time, Tuple(name, size))
    }).toSeq  //need Seq instead of Iterable
    
    //construct the Dataset, sort by creation time
    val md = Metadata("cache")
    val dataset = Dataset(Function(samples), md).sorted
    
    //apply the Operations
    operations.foldLeft(dataset)((ds, op) => op(ds))
  }
  
  
  /**
   * Release any resources that this accessor acquired.
   */
  def close(): Unit = {}
}
