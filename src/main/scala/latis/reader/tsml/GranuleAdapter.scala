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
 * Use an IterativeAdapter if you want to read data lazily.
 */
abstract class GranuleAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  /**
   * Read and cache the data.
   */
  override def init = cache(readData)

  /**
   * Subclasses need to implement this method to read all of the data
   * into the data Map.
   */
  def readData: Map[String, Data]
  
  
  /**
   * Override to construct Scalars using the data read by this Adapter and
   * stored in the cache.
   * Note that only Scalars can have Data with this column-oriented Adapter.
   */
  override protected def makeScalar(scalar: Scalar): Option[Scalar] = {
    val md = scalar.getMetadata
    
    val name = scalar.getName
    val data = getCachedData(name) match {
      case Some(d) => d
      case None => throw new Error("No data found in cache for Variable: " + name)
    }

    Some(Util.dataToVariable(data, scalar).asInstanceOf[Scalar])
    //TODO: use builder method on Variable
  }

}
