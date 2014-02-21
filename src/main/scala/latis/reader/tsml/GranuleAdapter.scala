package latis.reader.tsml

import latis.dm._
import scala.collection._
import latis.time.Time
import latis.reader.tsml.ml.ScalarMl
import latis.reader.tsml.ml.Tsml
import latis.data.EmptyData
import latis.data.Data
import latis.util.Util

/**
 * An Adapter for Datasets small enough to fit into memory.
 * This Adapter will read all the data in the Dataset and 
 * store it in a column-oriented store (i.e. each Scalar's
 * data are stored in a separate Data object).
 */
abstract class GranuleAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  //TODO: avoid reading data till requested? 
  //  but used to build dataset that is returned
  //  use Iterative adapter if you want to be lazy
  
  /**
   * Read data and combine with data values from tsml into the cache
   * then delegate to super.
   */
  override def getDataset: Dataset = {
    dataCache = (tsmlData ++ readData).toMap //make immutable
    super.getDataset
  }

  /**
   * Subclasses need to implement this method to read all of the data
   * into the dataMap.
   * This will be invoked lazily, when the dataMap is first accessed.
   */
  def readData: Map[String, Data]
  
  
  /**
   * Override to construct Scalars using the data read by this Adapter and
   * stored in the cache.
   * Note that only Scalars can have Data with this column-oriented Adapter.
   */
  override protected def makeScalar(scalar: Scalar): Option[Scalar] = {
    val md = scalar.getMetadata
    
    val name =  scalar.getName
    val data = dataCache.get(name) match {
      case Some(d) => d
      case None => throw new Error("No data found in cache for Variable: " + name)
    }
    
    /*
     * TODO: only getting one sample written even though they are all in the cache
     * make sure Function iterator can stitch these together
     */
    Some(Util.dataToVariable(data, scalar).asInstanceOf[Scalar])
    //TODO: use builder method on Variable
  }

}
