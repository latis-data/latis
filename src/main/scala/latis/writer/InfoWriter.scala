package latis.writer

import latis.dm._
import latis.reader.tsml.TsmlReader
import latis.ops.Selection

class InfoWriter extends TextWriter{
  
  override def write(dataset: Dataset){
    val info = getInfo(dataset).toStringMap
    writeLabel(info)
    writeDesc(info)
    printWriter.flush
  }

  def getInfo(dataset: Dataset): Dataset = {
    val reader = TsmlReader("datasets/test/lemr.tsml")
    reader.getDataset(Seq(Selection("query=PREFIX dcat:<http://www.w3.org/ns/dcat#> " + 
        "PREFIX dcterms:<http://purl.org/dc/terms/> PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#> "+
        "PREFIX vivo:<http://vivoweb.org/ontology/core#> SELECT ?desc?id?label " +
        "WHERE{?s a dcat:Dataset. ?s dcterms:identifier?id. ?s rdfs:label?label. "+
        "OPTIONAL{?s vivo:description?desc.} Filter(?id=\""+dataset.getName+"\")}")))
  }
  
  def writeLabel(info: scala.collection.Map[String,Array[String]]) {
    try{
      printWriter.print(info("object")(0)+"\n")
    } catch {
      case e : Exception => printWriter.print("No label found\n")
    }
  }
  
  def writeDesc(info: scala.collection.Map[String,Array[String]]) {
    try{
      printWriter.print(info("subject")(0))
    } catch {
      case e : Exception => printWriter.print("No description found")
    }
  }
}