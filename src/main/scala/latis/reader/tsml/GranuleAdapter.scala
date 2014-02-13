package latis.reader.tsml

import latis.dm._
import scala.collection._
import latis.time.Time
import latis.reader.tsml.ml.ScalarMl
import latis.reader.tsml.ml.Tsml
import latis.data.EmptyData

/**
 * An Adapter for Datasets small enough to fit into memory.
 * This Adapter will read all the data in the Dataset and 
 * store it in a column-oriented store (i.e. each Scalar's
 * data are stored in a separate Data object).
 */
abstract class GranuleAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  //TODO: don't make String specific, use Data? type Value = ...?
  
  //TODO: compare to DataMap util
  lazy val dataMap: immutable.Map[String, immutable.Seq[String]] = readData

  /**
   * Subclasses need to implement this method to read all of the data
   * into the dataMap.
   * This will be invoked lazily, when the dataMap is first accessed.
   */
  def readData: immutable.Map[String, immutable.Seq[String]]
  
  
  /**
   * Make a mutable data structure (variable name to Seq of values) to use while reading data.
   */
  def initDataMap: mutable.HashMap[String, mutable.ArrayBuffer[String]] = {
    val map = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    for (vname <- origVariableNames) map += ((vname, mutable.ArrayBuffer[String]()))
    map
  }
  
  /**
   * Return an immutable dataMap from the mutable data structure we use when reading data.
   */
  def immutableDataMap(map: mutable.HashMap[String, mutable.ArrayBuffer[String]]): immutable.Map[String, immutable.Seq[String]] = {
    val z = for ((name, seq) <- map) yield name -> seq.toIndexedSeq //turn ArrayBuffer into an immutable Seq
    z.toMap //turn HashMap into an immutable Map
  }
  
  
  /**
   * Override to construct Scalars using the data read by this Adapter.
   * Note that only Scalars can have Data with this column-oriented Adapter.
   */
  override protected def makeScalar(scalar: Scalar): Option[Scalar] = {
    val md = scalar.getMetadata
    
    val data = dataMap.getOrElse(md("name"), immutable.Seq[String]()) //TODO: error if name not found?

    scalar match {
      //TODO: can we delegate more to Time factory?
      case _: Time => {
        //if numeric units
        md.get("units") match {
          case Some(u) => {
            //support real or int
            if (u.contains(" since ")) {
              val values = md("type") match {
                case "integer" => data.map(_.toLong)
                case _ => data.map(_.toDouble) //default to double times
              }
              Some(Time(md, values))
            }
            else Some(Time.fromStrings(md, data))
          }
          case None => Some(Time(md, data.map(_.toDouble))) //default to double times
        }
      }
      case _: Real => Some(Real(md, data.map(_.toDouble)))
      case _: Integer => Some(Integer(md, data.map(_.toLong)))
      case _: Text => Some(Text(md, data))
      case _: Index => {
        //get the number of samples for one of the variables
        val length = dataMap.last._2.length
        Some(Index.withLength(length))
      }
      
      case _ => None
    }
  }

}