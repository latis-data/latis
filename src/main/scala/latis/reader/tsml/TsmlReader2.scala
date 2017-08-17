package latis.reader.tsml

import latis.dm._
import latis.ops.Operation
import latis.reader.DatasetAccessor

import java.net.URL

import scala.collection.Seq
import latis.reader.adapter.Adapter
import scala.xml.XML
import latis.reader.tsml.ml._
import latis.metadata.Metadata
import java.io.File
import java.nio.file.Path
import latis.util.LatisProperties
import latis.reader.adapter.ProcessingInstruction

/**
 * A Reader for a Dataset defined as a tsml file.
 * This will construct a dataset Model from the tsml and an Adapter
 * to generate the Dataset.
 */
class TsmlReader2(url: URL) extends DatasetAccessor {
  //TODO: dataset joins, nested datasets in tsml
  
  val tsml = Tsml(url)
    
  /**
   * Construct a Model from the tsml file.
   * Use the Tsml classes to parse the tsml, for now.
   * Do this eagerly so it fails fast.
   */
  val model: Model = {
    val dsml = tsml.dataset
    val vtype = varMlToVarType(dsml.getVariableMl)
    val md = makeMetadata(dsml)
    val pis = tsml.processingInstructions.map(p => ProcessingInstruction(p._1, p._2))
    Model(vtype, md, pis)
  }
  
  
  /**
   * Recursive function to build VariableType graph 
   * from VariableMl graph.
   */
  private def varMlToVarType(vml: VariableMl): VariableType = vml match {
    //TODO: put label in md as type?
    //TODO: put element atts into PIs and/or metadata?
    //TODO: deal with undefined name?
    case ml: ScalarMl =>
      val id = ml.getName
      val md = makeMetadata(ml)
      ScalarType(id, md)
    case ml: TimeMl => 
      val id = ml.getName
      val md = makeMetadata(ml)
      //add default type of "real"
      val md2 = if (! md.has("type")) md + ("type" -> "real") else md
      ScalarType(id, md2)
    case ml: TupleMl =>
      val vs = ml.variables.map(varMlToVarType(_))
      val id = ml.getName
      val md = makeMetadata(ml)
      TupleType(vs, id, md)
    case ml: FunctionMl =>
      val domain   = varMlToVarType(ml.domain)
      val codomain = varMlToVarType(ml.range)
      val id = ml.getName
      val md = makeMetadata(ml)
      FunctionType(domain, codomain, id, md)
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
  private var _adapter: Adapter = null
  
  /**
   * Singleton instance of Dataset in case the user calls getDataset more than once.
   * Note that we use an empty Dataset instead of None.
   * Thus, additional calls to getDataset will try again
   * if the original result was empty.
   */
  //TODO: what if getDataset called with diff ops?
  //TODO: force re-read? e.g. repopulate cache
  private var _dataset: Dataset = Dataset.empty
  
  /**
   * Return the LaTiS Dataset that tsml represents with the 
   * given Operations applied.
   */
  def getDataset(operations: Seq[Operation]): Dataset = {
    if (_dataset.isEmpty) _dataset = {
      _adapter = Adapter(model, tsml.dataset.getAdapterAttributes)
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

object TsmlReader2 {
  
  def fromURL(url: URL): TsmlReader2 = new TsmlReader2(url)

  def fromPath(path: String): TsmlReader2 = {
    val url = if (path.contains(":")) new URL(path) //already absolute with a scheme
    else {
      val file = LatisProperties.resolvePath(path)
      if (new File(file).exists) new URL("file:" + file)
      else throw new RuntimeException(s"TsmlReader unable to resolve path: $path")
    }
    fromURL(url)
  }
  
  def fromName(name: String): TsmlReader2 = {
    //TODO: search dataset.path?
    val path = LatisProperties.getOrElse("dataset.dir", "datasets") + 
               File.separator + name + ".tsml2"
    fromPath(path)
  }
  
}







