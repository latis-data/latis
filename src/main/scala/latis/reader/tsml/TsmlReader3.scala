package latis.reader.tsml

import latis.dm._
import latis.metadata._
import latis.ops.Operation
import latis.reader._
import latis.reader.adapter._
import latis.reader.tsml.ml._
import latis.util.LatisProperties

import scala.collection._
import scala.xml.XML
import java.io.File
import java.nio.file.Path
import java.net.URL

/**
 * A Reader for a Dataset defined as a tsml file.
 * This will construct a Dataset model from the tsml (without data) 
 * and an Adapter to generate the Dataset.
 */
class TsmlReader3(url: URL) extends DatasetSource {
  //TODO: dataset joins, nested datasets in tsml
  
  val tsml = Tsml(url)
    
  /**
   * Construct Metadata from the tsml file.
   * Use the Tsml classes to parse the tsml, for now.
   * Do this eagerly so it fails fast.
   */
  val metadata: Metadata3 = {
    val dsml = tsml.dataset
    val vmd = varMlToVariableMetadata(dsml.getVariableMl)
    val props = makeProperties(dsml)
    Metadata3(vmd)(props)
  }
  
  /**
   * Recursive function to build VariableType graph 
   * from VariableMl graph.
   */
  private def varMlToVariableMetadata(vml: VariableMl): VariableMetadata3 = vml match {
    //TODO: put label in md as type?
    //TODO: put element atts into PIs and/or metadata?
    //TODO: deal with undefined name?
    case ml: ScalarMl =>
      val props = makeProperties(ml)
      ScalarMetadata(props)
    case ml: TimeMl => 
      val props = makeProperties(ml)
      //add default type of "real"
      val props2 = if (! props.contains("type")) props + ("type" -> "real") else props
      ScalarMetadata(props2)
    case ml: TupleMl =>
      val vs = ml.variables.map(varMlToVariableMetadata(_))
      val props = makeProperties(ml)
      TupleMetadata(vs)(props)
    case ml: FunctionMl =>
      val domain   = varMlToVariableMetadata(ml.domain)
      val codomain = varMlToVariableMetadata(ml.range)
      val props = makeProperties(ml)
      FunctionMetadata(domain, codomain)(props)
  }
    
  private def makeProperties(vml: VariableMl): immutable.Map[String, String] = {
    /*
     * Combine tsml variable element label (type), attributes,
     *   and metadata attributes.
     * Note that some of these may need to become PIs.
     * 
     */
    val props = mutable.Map[String, String]()
    props ++= vml.getAttributes
    props ++= vml.getMetadataAttributes
    
    // if id = time, look for other "type" def or use "real"
    val typ = vml.label match {
      case "time" => props.get("type") match {
        case Some(t) => t
        case None => "real"
      }
      case s => s
    }
    props += "type" -> typ
    
    // Use "id" for "name" metadata
    if (! props.contains("name")) vml.getAttribute("id").foreach(id => props += ("name" -> id))
    // Add implicit names to aliases
    if (vml.label == "time") addName(props, "time")
    if (vml.label == "index") addName(props, "index")
    
    props.toMap //return immutable Map
  }
    
  /**
   * If the Map does not have a "name" property, set it.
   * Otherwise, add an "alias".
   */
  private def addName(props: mutable.Map[String, String], name: String): Unit = 
    props.get("name") match {
      //Note, this does not prevent duplicates, which shouldn't hurt.
      case Some(_) => props.get("alias") match {
        case Some(a) => props += ("alias" -> s"$a,$name") //append to list of aliases
        case None    => props += ("alias" -> name)        //add alias
      }
      case None => props += ("name" -> name)
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
    if (_dataset == null) _dataset = {
      _adapter = Adapter3(metadata, makeAdapterConfig)
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
               File.separator + name + ".tsml3"
    fromPath(path)
  }
  
}







