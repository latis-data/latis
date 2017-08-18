package latis.writer

import latis.dm._
import latis.util.LatisProperties
import java.io.File
import java.io.OutputStream
import scala.collection.immutable
import java.io.FileOutputStream
import latis.util.ReflectionUtils

/**
 * Base class for Dataset writers.
 */
class Writer3 {
  
  //---- Define abstract write method -----------------------------------------
  
  /**
   * Output the given Dataset in the desired form.
   */
  def write(dataset: Dataset3): Unit = {
    val f = (v: Variable3) => v match {
      case Scalar3(z) => print(s"${z()}, ")
      //TODO: new line for each sample
      case _ => 
    }
    dataset.foreach(f)
  }
  
//  def write(dataset: Dataset3): Unit = {
//    dataset match {
//      case Dataset3(v) => 
//        println(dataset)
//        v match {
//          case Scalar3(d) => 
//        }
//    }
//  }
}

//==== Companion Object =======================================================

object Writer3 {
  
}
