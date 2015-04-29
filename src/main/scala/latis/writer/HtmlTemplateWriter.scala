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
    // Figure out some basic info about the structure of the function in this dataset (if
    // there is one). For now, we really only want to graph functions of a very simple
    // format: a single scalar domain variable, and one or more scalar range variables.
    // Functions inside functions and other complicated things are not supported (we'll
    // just hide the chart).
    val (domainName: String, rangeVars: Seq[String], graphEnabled: Boolean) = dataset match {
      case Dataset(function: Function) => {
        
        // Attempt to get name of single scalar domain variable
        val domainName: String = function.getDomain match {
          case s: Scalar => s.getName
          case Tuple(s: Scalar) => s.getName
          case _ => ""
        }
        
        // Attempt to get name of simple scalar range variables
        val rangeNames: Seq[String] = function.getRange match {
          case s: Scalar => Seq(s.getName)
          case Tuple(vars) =>
            if (vars.forall(_.isInstanceOf[Scalar])) {
              vars.map(_.asInstanceOf[Scalar].getName)
            }
            else {
              Seq[String]()
            }
          case _ => Seq[String]()
        }
        
        // only turn on the chart if both of the above were successful
        val graphEnabled = (!domainName.equals("")) && (rangeNames.length > 0)
        
        (domainName, rangeNames, graphEnabled)
      }
      
      // top level Variable is not a function, return some boring default values
      case _ => ("", Seq[String](), false)
    }

    val defaultRangeName = rangeVars.headOption.getOrElse("")
    
    val values: Map[String, String] = Map(
        "long-name" -> dataset.getMetadata.getOrElse("long_name",dataset.getName),
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
    w.makeHeader(dataset) + w.varToString(dataset.unwrap) + w.makeFooter(dataset)
  }
  
  private def makeDas(dataset: Dataset): String = {
    val w = new DasWriter
    w.makeHeader(dataset) + w.varToString(dataset.unwrap) + w.makeFooter(dataset)
  }
  
  private def makeFormItems(dataset: Dataset): String = {
    makeFormItem(dataset.unwrap)
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