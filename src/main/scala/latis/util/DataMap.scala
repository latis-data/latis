package latis.util

import scala.collection.Map
import scala.collection.mutable

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Number
import latis.dm.Real
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.time.Time

/**
 * Utility methods for getting convenient data structures out of a Dataset.
 */
object DataMap {
  //TODO: exclude Index?
  //TODO: add aliases as keys pointing to the same values

  /**
   * Convert the Scalars within the given Dataset to a Map from name to Double array.
   */
  def toDoubleMap(dataset: Dataset): Map[String,Array[Double]] = {
    //construct a mutable data structure to build results
    val abmap = mutable.LinkedHashMap[String, mutable.ArrayBuffer[Double]]() //maintain order
  
    //Iterate through Dataset similar to Writer
    for (v <- dataset.getVariables) putDoublesInMap(v, abmap)
    
    //TODO: Make the Map immutable? but Array is mutable
    val amap = mutable.LinkedHashMap[String, Array[Double]]()
    for ((name, value) <- abmap) amap += ((name, value.toArray))
    amap
  }
  
  /**
   * Convert the Scalars within the given Dataset to a 2D array of Doubles
   * where the slower varying dimension represents the Variable.
   */
  def toDoubles(dataset: Dataset): Array[Array[Double]] = {
    //TODO: optimize by skipping the mapping step, especially since it requires named variables
    val map = toDoubleMap(dataset)
    val ab = mutable.ArrayBuffer[Array[Double]]()
    map.foldLeft(ab)(_ += _._2).toArray
  }
  
  /**
   * Recursively build the data Map.
   */
  private def putDoublesInMap(v: Variable, m: mutable.Map[String, mutable.ArrayBuffer[Double]]): Unit = v match {
    //case _: Index => //don't include Index
    case s: Scalar => {
      val name = v.getName
      if (name == "unknown") throw new Error("Scalar Variables must have names to be put into a data Map.")
      //get the buffer for this scalar, make a new one if it doesn't exist
      val buffer = m.get(name) match {
        case Some(ab) => ab
        case None => { //doesn't exist, make buffer and add it
          val ab = mutable.ArrayBuffer[Double]()
          m += ((name, ab))
          ab
        }
      }
      //Add double to buffer for Numbers, otherwise NaN
      v match {
        case Number(d) => buffer += d
        case t:Time => buffer += t.getJavaTime.toDouble
        case _ => buffer += Double.NaN
      }
    }
    
    case Tuple(vars) => for (v <- vars) putDoublesInMap(v, m)
    
    case Function(it) => for (sample <- it) putDoublesInMap(sample, m)
  }
      
  //=========================================================================
    
  /**
   * Convert the Scalars within the given Dataset to a Map from name to String array.
   */
  def toStringMap(dataset: Dataset): Map[String,Array[String]] = {
    //construct a mutable data structure to build results
    val abmap = mutable.LinkedHashMap[String, mutable.ArrayBuffer[String]]() //maintain order
  
    //Iterate through Dataset similar to Writer
    for (v <- dataset.getVariables) putStringsInMap(v, abmap)
    
    //TODO: Make the Map immutable? but Array is mutable
    val amap = mutable.LinkedHashMap[String, Array[String]]()
    for ((name, value) <- abmap) amap += ((name, value.toArray))
    amap
  }
    
  /**
   * Convert the Scalars within the given Dataset to a 2D array of Strings
   * where the slower varying dimension represents the Variable.
   */
  def toStrings(dataset: Dataset): Array[Array[String]] = {
    //TODO: optimize by skipping the mapping step
    val map = toStringMap(dataset)
    val ab = mutable.ArrayBuffer[Array[String]]()
    map.foldLeft(ab)(_ += _._2).toArray
  }
    
  /**
   * Recursively build the data Map.
   */
  private def putStringsInMap(v: Variable, m: mutable.Map[String, mutable.ArrayBuffer[String]]): Unit = v match {
    //case _: Index => //don't include Index
    case s: Scalar => {
      val name = v.getName
      if (name == "unknown") throw new Error("Scalar Variables must have names to be put into a data Map.")
      //get the buffer for this scalar, make a new one if it doesn't exist
      val buffer = m.get(name) match {
        case Some(ab) => ab
        case None => { //doesn't exist, make buffer and add it
          val ab = mutable.ArrayBuffer[String]()
          m += ((name, ab))
          ab
        }
      }
      //Add string value to buffer
      v match {
        case Index(i) => buffer += i.toString
        case Real(d) => buffer += d.toString
        case Integer(l) => buffer += l.toString
        case Text(s) => buffer += s.trim
      }
    }
    
    case Tuple(vars) => for (v <- vars) putStringsInMap(v, m)
    
    case Function(it) => for (sample <- it) putStringsInMap(sample, m)
  }
}
