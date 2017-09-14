package latis.writer

import latis.dm._
import latis.util._
import java.io._
import scala.collection._
import latis.data.value._

/**
 * Base class for Dataset writers.
 */
class Writer3 {
  
  private var outputStream: OutputStream = null
  private lazy val printWriter = new PrintWriter(outputStream)
  
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
    case s: Scalar3[_] => scalarToString(s)
    case t: Tuple3     => tupleToString(t)
    case f: Function3  => functionToString(f)
  }
  
  def scalarToString(scalar: Scalar3[_]): String = scalar match {
    case Text3(v)     => v
    case Integer3(v) => v.toString
    case Real3(v)    => v.toString
    case _           => ""
  }
  
  def tupleToString(tuple: Tuple3): String = tuple match {
    case Tuple3(vs @ _*) => vs.map(varToString(_)).mkString("(", ", ", ")")
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
  
  def apply(): Writer3 = Writer3(System.out)
  
  def apply(out: OutputStream): Writer3 = {
    val writer = new Writer3
    writer.outputStream = out
    writer
  }
}
