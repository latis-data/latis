package latis.writer

import latis.dm._

class HtmlWriter extends TextWriter {
  
  override def write(dataset: Dataset) {
    writeHeader(dataset)
    writeBody(dataset)
    printWriter.flush
  }
  
  override def makeHeader(dataset: Dataset): String = {
    val sb = new StringBuilder
    val name = dataset.getMetadata("long_name")
    sb append "<!DOCTYPE html>"
    sb append "\n<html lang=\"en-us\">"
    sb append "\n<head>"
    sb append s"\n<title>$name</title>"
    //sb append "\n<link rel=\"stylesheet\" type=\"text/css\" href=\"\" />"
    sb append scripts
    sb append "\n</head>\n"    
    sb.toString
  }
  
  val scripts = "\n<script type=\"text/javascript\" src=\"/lisird/scripts/js/plots.js\"></script>\n<script type=\"text/javascript\" src=\"/lisird/scripts/LisirdDygraph/TimeseriesDygraph.js\"></script>\n<script type=\"text/javascript\" src=\"/lisird/scripts/LisirdDygraph/HistoricalTsiDygraph.js\"></script>"
  
  def writeBody(dataset: Dataset) = printWriter.print(makeBody(dataset))
  
  def makeBody(dataset: Dataset): String = {
    val name = dataset.getMetadata("long_name")
    val sb = new StringBuilder
    sb append "\n<body>"
    sb append s"\n<h1>$name</h1>"
    sb append makeInfo(dataset)
    sb append query
    
    
    sb.toString
  }
  
  def makeInfo(dataset: Dataset): String = {
    val sb = new StringBuilder
    sb append "\n<div id=\"info\">"
    val w = new InfoWriter
    val info = w.getInfo(dataset).toStringMap
    sb append info("object")(0)
    sb append info("predicate")(0)
    sb append "\n</div>"
    sb.toString
  }

  val query = "\n<form name=\"time\">Year (yyyy-mm-dd or years since 0000-01-01)<br/>Select Range: <input type=\"text\" name=\"time1\" /><input type=\"text\" name=\"time2\" /><br/></form><br/>\nSelect Output Type: <select id=\"output\"><option value=\"csv\">csv</option><br/><option value=\"dods\">dods</option><br/><option value=\"html\">html</option><br/><option value=\"dat\">dat</option><br/><option value=\"bin\">bin</option><br/><option value=\"json\">json</option><br/><option value=\"info\">info</option><br/><option value=\"das\">das</option><br/><option value=\"dds\">dds</option><br/></select><br/>\n<input type=\"button\" value=\"Submit\" onclick=\"handle_dataset_request()\"/>"
  
  override def mimeType: String = getProperty("mimeType","text/html")
}