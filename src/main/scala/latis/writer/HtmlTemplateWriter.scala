package latis.writer

import sun.misc.IOUtils
import scala.io.Source
import latis.dm._


/**
 * A Writer that outputs HTML. Outputs the same thing as HtmlWriter
 * (i.e. an HTML summary of a dataset), but uses templates for
 * (hopefully) improved readability.
 * @author brpu2352
 */
class HtmlTemplateWriter extends TextWriter {
  
  private val mainTmpl = StringTemplate.fromResource("templates/htmlwriter/main.html")
  private val formItemTmpl = StringTemplate.fromResource("templates/htmlwriter/formItem.html")
  
  override def mimeType: String = getProperty("mimeType", "text/html")
  
  override def write(dataset: Dataset) {
    
    val function = dataset.findFunction
    val (domainName: String, rangeVars: Seq[String]) = function match {
      case Some(f) => (f.getDomain.getName, f.getRange.toSeq.view.flatMap(_.getMetadata("name")))
      case None => ("", Seq())
    }
    val defaultRangeName = rangeVars.headOption.getOrElse("")
    
    val values: Map[String, String] = Map(
        "long-name" -> dataset.getMetadata("long_name").getOrElse(dataset.getName),
        "name" -> dataset.getName,
        "domain" -> domainName,
        "defaultRange" -> defaultRangeName,
        "dds" -> makeDds(dataset),
        "das" -> makeDas(dataset),
        "form-items" -> makeFormItems(dataset),
        "output-select-option-elements" -> selectOptions,
        "range-select-option-elements" -> rangeVars.map(name => s"""<option value="$name">$name</option>""").mkString
    )
    
    printWriter.print(mainTmpl.applyValues(values))
    printWriter.flush
  }
  
  private def makeDds(dataset: Dataset): String = {
    val w = new DdsWriter
    w.makeHeader(dataset)+dataset.getVariables.map(w.varToString(_)).mkString("")+w.makeFooter(dataset)
  }
  
  private def makeDas(dataset: Dataset): String = {
    val w = new DasWriter
    w.makeHeader(dataset)+dataset.getVariables.map(w.varToString(_)).mkString("")+w.makeFooter(dataset)
  }
  
  private def makeFormItems(dataset: Dataset): String = {
    dataset.getVariables.map(makeFormItem(_)).mkString
  }
  
  private def makeFormItem(v: Variable): String = v match {
    case s: Scalar => applyFormTemplate(s)
    case Tuple(vars) => vars.map(makeFormItem(_)).mkString
    case f: Function => makeFormItem(Sample(f.getDomain, f.getRange))
  }
  private def applyFormTemplate(s: Scalar): String = {
    val values = Map(
        "name" -> s.getName,
        "units" -> s.getMetadata("units").getOrElse("unknown units")
    )
    formItemTmpl.applyValues(values)
  }
  
  private val suffixes = List(
    "asc", "bin", "csv", "das", "dds", "dods", "html", "info",
    "json", "jsond", "meta", "png", "txt")
  private val selectOptions = suffixes.map(suf => s"""<option value="$suf">$suf</option>""").mkString
  
}