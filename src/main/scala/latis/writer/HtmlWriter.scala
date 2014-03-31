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
    sb append "\n<link rel=\"stylesheet\" type=\"text/css\" href=\"\" />" //find out what href is
    sb append "\n<script>" // see what scripts I should use
    sb append "\n</head>\n"    
    sb.toString
  }
  
  def writeBody(dataset: Dataset) = printWriter.print(makeBody(dataset))
  
  def makeBody(dataset: Dataset): String = {
    val name = dataset.getMetadata("long_name")
    val sb = new StringBuilder
    sb append "\n<body>"
    sb append s"\n<h1>$name</h1>"
    sb append makeInfo(dataset)
    sb append ""
    
    
    sb.toString
  }
  
  def makeInfo(dataset: Dataset): String = {
    val sb = new StringBuilder
    sb append "\n<div id=\"info\">"
    //have to get the info here somehow
    sb append "\n</div>"
    sb.toString
  }

  override def mimeType: String = getProperty("mimeType","text/html")
}