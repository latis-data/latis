package latis.util

import latis.dm._
import scala.collection._
//import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

object DataMap {
  
  //TODO: exclude Index?
  
  //TODO: generalize data type
//  def toDataMap[A: ClassTag](dataset: Dataset): Map[String,Array[A]] = {
//    //TODO: what about 2D...? leave it to use to do index math
//    //TODO: can't have more than one "unknown", append number?
//    
//    //construct a mutable data structure to build results
//    val abmap = mutable.LinkedHashMap[String, mutable.ArrayBuffer[A]]() //maintain order
//  
//    //Iterate through Dataset similar to Writer
//    for (v <- dataset.getVariables) putDataInMap[A](v, abmap)
//    
//    //TODO: Make the Map immutable? but Array is mutable
//    val amap = mutable.LinkedHashMap[String, Array[A]]()
//    for ((name, value) <- abmap) amap += ((name, value.toArray))
//    amap
//  }
//    
//  private def putDataInMap[A](v: Variable, m: mutable.Map[String, mutable.ArrayBuffer[A]]): Unit = v match {
//    case s: Scalar => {
//      val name = v.getName
//      if (name == "unknown") throw new Error("Scalar Variables must have names to be put into a data Map.")
//      //get the buffer for this scalar, make a new one if it doesn't exist
//      val buffer = m.get(name) match {
//        case Some(ab) => ab
//        case None => { //doesn't exist, make buffer and add it
//          val ab = mutable.ArrayBuffer[A]()
//          m += ((name, ab))
//          ab
//        }
//      }
//      //Add double to buffer for Numbers, otherwise NaN
////      v match {
////        case Number(d) => buffer += d
////        case _ => buffer += Double.NaN
////      }
//      getValue(v: Variable)
//    }
//    
//    case Tuple(vars) => for (v <- vars) putDataInMap(v, m)
//    
//    case f: Function => for (sample <- f.iterator) putDataInMap(sample, m)
//  }
//  
//  private def getValue[A](v: Variable): A = ???
  
  
  /**
   * Convert the Scalars within this Variable to a Map from name to Double array.
   */
  def toDoubleMap(dataset: Dataset): Map[String,Array[Double]] = {
    //TODO: what about 2D...? leave it to use to do index math
    //TODO: can't have more than one "unknown", append number?
    
    //construct a mutable data structure to build results
    val abmap = mutable.LinkedHashMap[String, mutable.ArrayBuffer[Double]]() //maintain order
  
    //Iterate through Dataset similar to Writer
    for (v <- dataset.getVariables) putDoublesInMap(v, abmap)
    
    //TODO: Make the Map immutable? but Array is mutable
    val amap = mutable.LinkedHashMap[String, Array[Double]]()
    for ((name, value) <- abmap) amap += ((name, value.toArray))
    amap
  }
  
  def toDoubles(dataset: Dataset): Array[Array[Double]] = {
    //TODO: optimize by skipping the mapping step
    val map = toDoubleMap(dataset)
    val ab = mutable.ArrayBuffer[Array[Double]]()
    map.foldLeft(ab)(_ += _._2).toArray
    //too much?: toDoubleMap(dataset).foldLeft(mutable.ArrayBuffer[Array[Double]]())(_ += _._2).toArray
  }
  
  private def putDoublesInMap(v: Variable, m: mutable.Map[String, mutable.ArrayBuffer[Double]]): Unit = v match {
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
        case _ => buffer += Double.NaN
      }
    }
    
    case Tuple(vars) => for (v <- vars) putDoublesInMap(v, m)
    
    case f: Function => for (sample <- f.iterator) putDoublesInMap(sample, m)
  }
      
  //=========================================================================
    
  def toStringMap(dataset: Dataset): Map[String,Array[String]] = {
    //TODO: what about 2D...? leave it to use to do index math
    //TODO: can't have more than one "unknown", append number?
    
    //construct a mutable data structure to build results
    val abmap = mutable.LinkedHashMap[String, mutable.ArrayBuffer[String]]() //maintain order
  
    //Iterate through Dataset similar to Writer
    for (v <- dataset.getVariables) putStringsInMap(v, abmap)
    
    //TODO: Make the Map immutable? but Array is mutable
    val amap = mutable.LinkedHashMap[String, Array[String]]()
    for ((name, value) <- abmap) amap += ((name, value.toArray))
    amap
  }
  
  def toStrings(dataset: Dataset): Array[Array[String]] = {
    //TODO: optimize by skipping the mapping step
    val map = toStringMap(dataset)
    val ab = mutable.ArrayBuffer[Array[String]]()
    map.foldLeft(ab)(_ += _._2).toArray
    //too much?: toDoubleMap(dataset).foldLeft(mutable.ArrayBuffer[Array[Double]]())(_ += _._2).toArray
  }
  
  private def putStringsInMap(v: Variable, m: mutable.Map[String, mutable.ArrayBuffer[String]]): Unit = v match {
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
      //Add double to buffer for Numbers, otherwise NaN
      v match {
        case Index(i) => i.toString
        case Real(d) => d.toString
        case Integer(l) => l.toString
        case Text(s) => s.trim
      }
    }
    
    case Tuple(vars) => for (v <- vars) putStringsInMap(v, m)
    
    case f: Function => for (sample <- f.iterator) putStringsInMap(sample, m)
  }
}