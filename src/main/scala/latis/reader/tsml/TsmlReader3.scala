package latis.reader.tsml

import latis.dm._
import latis.metadata.Metadata
import latis.ops.Operation
import latis.reader.DatasetAccessor
import latis.reader.adapter._
import latis.reader.tsml.ml._
import latis.util.LatisProperties

import scala.collection.Seq
import scala.xml.XML
import java.io.File
import java.nio.file.Path
import java.net.URL

/**
 * A Reader for a Dataset defined as a tsml file.
 * This will construct a Dataset model from the tsml (without data) 
 * and an Adapter to generate the Dataset.
 */
class TsmlReader3(url: URL) { //TODO: extends DatasetSource {
  //TODO: dataset joins, nested datasets in tsml
  
  val tsml = Tsml(url)
    
  /**
   * Construct a Model from the tsml file.
   * Use the Tsml classes to parse the tsml, for now.
   * Do this eagerly so it fails fast.
   */
  val model: Dataset3 = {
    val dsml = tsml.dataset
    val variable = varMlToVariable(dsml.getVariableMl)
    val md = makeMetadata(dsml)
    Dataset3(dsml.getName, variable, md)
  }
  
  
  /**
   * Recursive function to build VariableType graph 
   * from VariableMl graph.
   */
  private def varMlToVariable(vml: VariableMl): Variable3 = vml match {
    //TODO: put label in md as type?
    //TODO: put element atts into PIs and/or metadata?
    //TODO: deal with undefined name?
    case ml: ScalarMl =>
      val id = ml.getName
      val md = makeMetadata(ml)
      Scalar3(id, md)
    case ml: TimeMl => 
      val id = ml.getName
      val md = makeMetadata(ml)
      //add default type of "real"
      val md2 = if (! md.has("type")) md + ("type" -> "real") else md
      Scalar3(id, md2)
    case ml: TupleMl =>
      val vs = ml.variables.map(varMlToVariable(_))
      val id = ml.getName
      val md = makeMetadata(ml)
      Tuple3(id, md, vs)
    case ml: FunctionMl =>
      val domain   = varMlToVariable(ml.domain)
      val codomain = varMlToVariable(ml.range)
      val id = ml.getName
      val md = makeMetadata(ml)
      Function3(id, md, domain, codomain)
  }
    
  private def makeMetadata(vml: VariableMl): Metadata = {
    /*
     * Combine tsml variable element label (type), attributes,
     *   and metadata attributes.
     * Note that some of these may need to become PIs.
     * 
     */
    val atts = scala.collection.mutable.Map[String, String]()
    atts ++= vml.getAttributes
    atts ++= vml.getMetadataAttributes
    
    // if id = time, look for other "type" def or use "real"
    val typ = vml.label match {
      case "time" => atts.get("type") match {
        case Some(t) => t
        case None => "real"
      }
      case s => s
    }
    atts += "type" -> typ
    
    var md = Metadata(atts)
    
    // Add implicit names
    // Use "id" for "name" metadata
    if (! md.has("name")) vml.getAttribute("id").foreach(id => md = md + ("name" -> id))
    if (vml.label == "time") md = md.addName("time")
    if (vml.label == "index") md = md.addName("index")
    
    md
  }
  
  /**
   * The adapter as defined in the TSML for reading the Dataset.
   */
  private var _adapter: Adapter3 = null
  
  def makeAdapterConfig: AdapterConfig = {
    val props = tsml.dataset.getAdapterAttributes
    val pis = tsml.processingInstructions.map(p => ProcessingInstruction(p._1, p._2))
    AdapterConfig(props, pis)
  }
  
  /**
   * Singleton instance of Dataset in case the user calls getDataset more than once.
   * Note that we use an empty Dataset instead of None.
   * Thus, additional calls to getDataset will try again
   * if the original result was empty.
   */
  //TODO: what if getDataset called with diff ops?
  //TODO: force re-read? e.g. repopulate cache
  private var _dataset: Dataset3 = null  //Dataset3.empty
  
  /**
   * Return the LaTiS Dataset that tsml represents with the 
   * given Operations applied.
   */
  def getDataset(operations: Seq[Operation]): Dataset3 = {
    if (_dataset != null) _dataset = {
      _adapter = Adapter3(model, makeAdapterConfig)
      _adapter.getDataset(operations)
    }
    else _dataset
    _dataset
  }
  
  /**
   * Clean up any resources that the adapter used.
   */
  def close: Unit = if (_adapter != null) _adapter.close

}

object TsmlReader3 {
  
  def fromURL(url: URL): TsmlReader3 = new TsmlReader3(url)

  def fromPath(path: String): TsmlReader3 = {
    val url = if (path.contains(":")) new URL(path) //already absolute with a scheme
    else {
      val file = LatisProperties.resolvePath(path)
      if (new File(file).exists) new URL("file:" + file)
      else throw new RuntimeException(s"TsmlReader unable to resolve path: $path")
    }
    fromURL(url)
  }
  
  def fromName(name: String): TsmlReader3 = {
    //TODO: search dataset.path?
    val path = LatisProperties.getOrElse("dataset.dir", "datasets") + 
               File.separator + name + ".tsml2"
    fromPath(path)
  }
  
}







