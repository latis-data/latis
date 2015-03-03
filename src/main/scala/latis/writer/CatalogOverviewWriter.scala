package latis.writer

import latis.dm.Dataset
import latis.dm.Binary
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import java.io.PrintWriter
import latis.dm.TupleMatch
import scala.annotation.meta.companionObject

class CatalogOverviewWriter extends Writer {
  
  private lazy val printer = new PrintWriter(getOutputStream)
  private def companion = CatalogOverviewWriter
  
  private def makeHeaderElement(variable: Variable, level: Int): String = {
    s"<h$level>${variable.getName}</h$level>"
  }
  
  def write(dataset: Dataset) {
    dataset.getVariables.foreach(v => writeVariable(v))
    printer.flush()
  }
  
  private def writeVariable(v: Variable): Unit = v match {
    case f: Function => writeFunction(f)
    case _ => {
      val str = varToString(v)
      printer.println(str)
    }
  }
  
  private def writeFunction(f: Function) {
    f.iterator.foreach(s => {
      val str = varToString(s)
      printer.println(str)
    })
  }
  
  private def varToString(v: Variable): String = v match {
    case   scalar: Scalar   => makeScalar(scalar)
    case   sample: Sample   => makeSample(sample)
    case    tuple: Tuple    => makeTuple(tuple)
    case function: Function => makeFunction(function)
  }
  
  private def makeScalar(s: Scalar): String = s match {
    case Index(i)   => i.toString
    case Real(d)    => d.toString
    case Integer(l) => l.toString
    case Text(s)    => s.trim
    case Binary(b)  => "blob"
  }
  
  private def makeSample(s: Sample): String = {
    makeTuple(s)
  }
  
  private def makeTuple(t: Tuple): String = t match {
    case Sample(d: Index, r) => varToString(r) //drop Index domain
    case TupleMatch(name:Text, f:Function) => {
      companion.nestedTmpl.applyValues(Map(
          "name" -> name.stringValue,
          "children" -> makeFunction(f)
      ))
    }
    case TupleMatch(name: Text, description: Text, TupleMatch(accessUrl: Text)) => {
      companion.leafTmpl.applyValues(Map(
          "name" -> name.stringValue,
          "description" -> description.stringValue,
          "accessUrl" -> ("latis/" + accessUrl.stringValue)
      ))
    }
    case Tuple(vars) => vars.map(varToString(_)).mkString
  }
  
  private def makeFunction(f: Function): String = f.iterator.map(varToString(_)).mkString("\n")
  
}

object CatalogOverviewWriter {
  def apply() = new CatalogOverviewWriter
  
  private val nestedTmpl = StringTemplate.fromResource("templates/overviewwriter/catalogNestedItem.html")
  private val leafTmpl = StringTemplate.fromResource("templates/overviewwriter/catalogLeafItem.html")
}