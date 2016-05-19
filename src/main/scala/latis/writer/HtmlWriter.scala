package latis.writer

import latis.dm._

/**
 * Writes an html landing page for a dataset.
 */
class HtmlWriter extends TextWriter {
  
  override def write(dataset: Dataset) {
    writeHeader(dataset)
    writeBody(dataset)
    printWriter.flush
  }
  
  /**
   * Makes the header of the html
   */
  override def makeHeader(dataset: Dataset): String = {
    val sb = new StringBuilder
    val name = dataset.getMetadata.getOrElse("long_name", dataset.getName)
    sb append "<!DOCTYPE html>"
    sb append "\n<html lang=\"en-us\">"
    sb append "\n<head>"
    sb append s"\n<title>$name</title>"
    sb append style
    sb append scripts
    sb append "\n</head>\n"    
    sb.toString
  }

  val scripts = "\n<script type=\"text/javascript\" src=\"http://lasp.colorado.edu/lisird/tss/resources/tss.js\"></script>" +
                "\n<script type=\"text/javascript\" src=\"http://dygraphs.com/dygraph-combined.js\"></script>"

  val style = "\n<link rel=\"stylesheet\" type=\"text/css\" href=\"http://lasp.colorado.edu/lisird/tss/resources/tss.css\">"  
    
  def writeBody(dataset: Dataset) = printWriter.print(makeBody(dataset))
  
  /**
   * The body includes a description of the data if available, 
   * the DDS and DAS dataset structure descriptions,
   * and a query form for selection of each variable.
   */
  def makeBody(dataset: Dataset): String = {
    val name = dataset.getMetadata.getOrElse("long_name", dataset.getName)
    val sb = new StringBuilder
    sb append "\n<body>"
    sb append s"\n<h1>$name</h1>"
    //sb append makeInfo(dataset)
    sb append makeDygraph(dataset)
    sb append dds(dataset)
    sb append das(dataset)
    sb append queryForms(dataset)
    
    sb append "\n</body>"
    sb append "\n</html>"
    sb.toString
  }

  //  /**
  //   * Uses the InfoWriter to get a dataset description.
  //   */
  //  def makeInfo(dataset: Dataset): String = {
  //    val sb = new StringBuilder
  //    val w = new InfoWriter
  //    val info = w.getInfo(dataset).toStringMap
  //    sb append "\n<blockquote>"
  //    sb append "\n<div class=\"info\">\n"
  //    sb append w.makeDesc(info)
  //    sb append "\n</div>"
  //    sb append "\n</blockquote>"
  //    sb.toString
  //  }

  def makeDygraph(dataset: Dataset): String = {
    val sb = new StringBuilder
    val name = dataset.getName
    dataset match {
      case Dataset(v) => v match {
        case f: Function => {
          sb append "\n<div id=\"graphdiv\"></div>"
          sb append "\n<script type=\"text/javascript\">"
          sb append "\ng = new Dygraph("
          sb append "\ndocument.getElementById(\"graphdiv\"),"
          sb append "\n\"" + name + ".csv\","
          sb append "\n{"
          sb append "\ndelimiter: ',',"
          sb append "\nxlabel: '" + f.getDomain.getName + "',"
          sb append "\nylabel: '" + f.getRange.getName + "',"
          sb append "\n});"
          sb append "\n</script>"
        }
        case _ => //no plot if no function
      }
      case _ => ""
    }
    sb.result
  }
  
  /**
   * Uses DdsWriter to show the structure of the dataset.
   */
  def dds(dataset: Dataset): String = {
    val w = new DdsWriter
    val sb = new StringBuilder
    val v = dataset match {
      case Dataset(v) => w.varToString(v).mkString("")
      case _ => ""
    }
    sb append "\n<div class=\"dds\">"
    sb append "\n<h2>Dataset Descriptor Structure</h2>"
    sb append "<blockquote>"
    sb append w.makeHeader(dataset) + v +w.makeFooter(dataset)
    sb append "\n</blockquote>"
    sb append "\n</div>"
    sb.toString
  }
  
  /**
   * Uses DasWriter to show metadata.
   */
  def das(dataset: Dataset): String = {
    val w = new DasWriter
    val sb = new StringBuilder
    val v = dataset match {
      case Dataset(v) => w.varToString(v).mkString("")
      case _ => ""
    }
    sb append "\n<div class=\"das\">"
    sb append "\n<h2>Dataset Attribute Structure</h2>"
    sb append "<blockquote>"
    sb append w.makeHeader(dataset) + v +w.makeFooter(dataset)
    sb append "\n</blockquote>"
    sb append "\n</div>"
    sb.toString
  }
  
  /**
   * Makes a form for retrieval of data in any supported format.
   */
  def queryForms(dataset: Dataset) = {
    val sb = new StringBuilder
    val v = dataset match {
      case Dataset(v) => makeForm(v)
      case _ => ""
    }
    sb append "\n<h2>Data Set Query Form</h2>"
    sb append v
    sb append "\nSelect Output Type: <select id=\"output\">"
    val suffix = List("asc", "bin", "csv", "das", "dds", "dods", "html", "info", "json", "jsond", "meta", "png", "txt")
    for(suf <- suffix) sb append "<option value=\"" + suf + "\">" + suf + "</option><br/>"
    sb append "</select><br/>\n<input type=\"button\" value=\"Submit\" onclick=\"handle_dataset_request()\"/>"
    sb.toString
  }
  
  /**
   * Makes a form for a selection operation.
   */
  def makeForm(variable: Variable): String = variable match {
    case s:Scalar => "\n<form name=\"" + s.getName + "\">" + s.getName + " (" + s.getMetadata("units").getOrElse("unknown units") + ")" + "<br/>Select Range: <input type=\"text\" name=\"x1\" /><input type=\"text\" name=\"x2\" /><br/></form><br/>"
    case Tuple(vars) => vars.map(makeForm(_)).mkString("")
    case function:Function => makeForm(Sample(function.getDomain, function.getRange))
  }
  
  override def mimeType: String = getProperty("mimeType","text/html")
}
