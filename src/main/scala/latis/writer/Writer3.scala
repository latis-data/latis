package latis.writer

import latis.dm._
import latis.util.LatisProperties
import java.io.File
import java.io.OutputStream
import scala.collection.immutable
import java.io.FileOutputStream
import latis.util.ReflectionUtils
import latis.data.value._

/**
 * Base class for Dataset writers.
 */
class Writer3 {
  
  //---- Define abstract write method -----------------------------------------
  
  /**
   * Output the given Dataset in the desired form.
   */
  def write(dataset: Dataset3): Unit = {
    println(dataset) //TODO: header
    val f = (s: Sample3) => writeSample(s)
    dataset.foreach(f)
  }
  
  def writeSample(sample: Sample3): Unit = {
    println(sampleToString(sample))
  }
  
  def sampleToString(sample: Sample3): String = sample match {
    case Sample3(domain, range) =>
      varToString(domain) + " -> " + varToString(range)
  }
  
  def varToString(variable: Variable3): String = variable match {
    case s: Scalar3   => scalarToString(s)
    case t: Tuple3    => tupleToString(t)
    case f: Function3 => functionToString(f)
  }
  
  def scalarToString(scalar: Scalar3): String = scalar match {
    case Scalar3(d) => d() match {
      case StringValue(v) => v
      case LongValue(v) => v.toString
      case DoubleValue(v) => v.toString
      //TODO: have all extend DataValue with a toString,...?
    }
    case _ => ""
  }
  
  def tupleToString(tuple: Tuple3): String = tuple match {
    case Tuple3(vs) => vs.map(varToString(_)).mkString("(", ", ", ")")
    case _ => ""
  }
  
  /**
   * Platform independent new-line.
   */
  val newLine = System.getProperty("line.separator") //"\n"
  //TODO: put in StringUtils?
  
  //nested function
  //TODO: indent
  def functionToString(function: Function3): String = function match {
    case SampledFunction3(it) => 
      val ss = it.map(sampleToString(_))
      ss.mkString(s"{$newLine", newLine, s"}$newLine")
    case _ => ""
  }

}

//==== Companion Object =======================================================

object Writer3 {
  
}
