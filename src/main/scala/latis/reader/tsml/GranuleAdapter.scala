package latis.reader.tsml

import latis.dm._
import scala.collection._
import latis.time.Time

/**
 * An Adapter for Datasets small enough to fit into memory.
 * This Adapter will read all the data in the Dataset and 
 * store it in a column-oriented store (i.e. each Scalar's
 * data are stored in a separate Data object).
 */
abstract class GranuleAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  //TODO: dataMap should contain Data, impl Data with Seq[String]?
  lazy val dataMap: immutable.Map[String, immutable.Seq[String]] = readData

  /**
   * Subclasses need to implement this method to read all of the data
   * into the dataMap.
   * This will be invoked lazily, when the dataMap is first accessed.
   */
  def readData: immutable.Map[String, immutable.Seq[String]]
  
  /**
   * Override to construct Scalars using the data read by this Adapter.
   * Note that only Scalars can have Data with this column-oriented Adapter.
   */
  override protected def makeScalar(sml: ScalarMl): Option[Scalar] = {
    val md = makeMetadata(sml)
    
    val data = dataMap(md("name"))
        
    sml.label match {
      case "real" => Some(Real(md, data.map(_.toDouble)))
      case "time" => Some(Time(md, data.map(_.toDouble)))
      
      //TODO: *** other types ***
      
      case _ => None
    }
  }

}