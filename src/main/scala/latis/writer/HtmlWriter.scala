package latis.writer

import latis.dm._
import java.io.File
import java.io.FileOutputStream
import latis.reader.tsml.TsmlReader

class HtmlWriter extends TextWriter {
  
  override def write(dataset: Dataset) {
    writeHeader(dataset)
    writeBody(dataset)
    printWriter.flush
  }
  
  override def makeHeader(dataset: Dataset): String = {
    val sb = new StringBuilder
    val name = dataset.getMetadata("long_name").get
    sb append "<!DOCTYPE html>"
    sb append "\n<html lang=\"en-us\">"
    sb append "\n<head>"
    sb append s"\n<title>$name</title>"
    sb append style
    sb append scripts
    sb append "\n</head>\n"    
    sb.toString
  }
  
  val scripts = ""
  
  val style = "\n<link rel=\"stylesheet\" type=\"text/css\" href=\"http://lasp.colorado.edu/lisird/tss/resources/tss.css\">"  
    
  def writeBody(dataset: Dataset) = printWriter.print(makeBody(dataset))
  
  def makeBody(dataset: Dataset): String = {
    val name = dataset.getMetadata("long_name").get
    val sb = new StringBuilder
    sb append "\n<body>"
    sb append s"\n<h1>$name</h1>"
    sb append makeInfo(dataset)
    sb append dds(dataset)
    sb append das(dataset)
    //sb append image(dataset)
    sb append queryForms(dataset)
    
    sb append "\n</body>"
    sb append "\n</html>"
    sb.toString
  }
  
  def makeInfo(dataset: Dataset): String = {
    val sb = new StringBuilder
    val w = new InfoWriter
    val info = w.getInfo(dataset).toStringMap
    sb append "\n<blockquote>"
    sb append "\n<div class=\"info\">\n"
    sb append w.makeDesc(info)
    sb append "\n</div>"
    sb append "\n</blockquote>"
    sb.toString
  }
  
  def dds(dataset: Dataset): String = {
    val w = new DdsWriter
    val sb = new StringBuilder
    sb append "\n<div class=\"dds\">"
    sb append "\n<h2>Dataset Descriptor Structure</h2>"
    sb append "<blockquote>"
    sb append w.makeHeader(dataset)+dataset.getVariables.map(w.varToString(_)).mkString("")+w.makeFooter(dataset)
    sb append "\n</blockquote>"
    sb append "\n</div>"
    sb.toString
  }
  
  def das(dataset: Dataset): String = {
    val w = new DasWriter
    val sb = new StringBuilder
    sb append "\n<div class=\"das\">"
    sb append "\n<h2>Dataset Attribute Structure</h2>"
    sb append "<blockquote>"
    sb append w.makeHeader(dataset)+dataset.getVariables.map(w.varToString(_)).mkString("")+w.makeFooter(dataset)
    sb append "\n</blockquote>"
    sb append "\n</div>"
    sb.toString
  }
  
  def image(dataset: Dataset): String = {
    val sb = new StringBuilder
    val file = new File("/tmp/html_image.png")
    file.createNewFile
    val fos = new FileOutputStream(file)
    val ds = TsmlReader("datasets/test/historical_tsi.tsml").getDataset
    Writer(fos, "png").write(ds)
    fos.close()
    sb append "\n<img src=\"" + file + "\" alt=\"no image found\">"
    sb.toString
  }

  def queryForms(dataset: Dataset) = {
    val sb = new StringBuilder
    sb append "\n<h2>Data Set Query Form</h2>"
    for(v <- dataset.getVariables) sb append makeForm(v)
    sb append "\nSelect Output Type: <select id=\"output\"><option value=\"asc\">asc</option><br/><option value=\"bin\">bin</option><br/><option value=\"csv\">csv</option><br/><option value=\"das\">das</option><br/><option value=\"dds\">dds</option><br/><option value=\"dods\">dods</option><br/><option value=\"html\">html</option><option value=\"info\">info</option><option value=\"json\">json</option><br/><option value=\"jsond\">jsond</option><br/><option value=\"meta\">meta</option><option value=\"png\">png</option><option value=\"txt\">txt</option><br/></select><br/>\n<input type=\"button\" value=\"Submit\" onclick=\"handle_dataset_request()\"/>"
    sb.toString
  }
  
  def makeForm(variable: Variable): String = variable match {
    case s:Scalar => "\n<form name=\"" + s.getName + "\">" + s.getName + " (" + s.getMetadata("units").getOrElse("unknown units") + ")" + "<br/>Select Range: <input type=\"text\" name=\"x1\" /><input type=\"text\" name=\"x2\" /><br/></form><br/>"
    case Tuple(vars) => vars.map(makeForm(_)).mkString("")
    case function:Function => makeForm(Sample(function.getDomain, function.getRange))
  }
  
  override def mimeType: String = getProperty("mimeType","text/html")
}