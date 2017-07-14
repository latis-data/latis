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
    Model(varMlToVarType(tsml.dataset.getVariableMl))
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
      val md = Metadata(ml.getMetadataAttributes)
      ScalarType(id, md)
    //TODO: TimeMl, adds default type of "real"
    case ml: TupleMl =>
      val vs = ml.variables.map(varMlToVarType(_))
      val id = ml.getName
      val md = Metadata(ml.getMetadataAttributes)
      TupleType(vs, id, md)
    case ml: FunctionMl =>
      val domain   = varMlToVarType(ml.domain)
      val codomain = varMlToVarType(ml.range)
      val id = ml.getName
      val md = Metadata(ml.getMetadataAttributes)
      FunctionType(domain, codomain, id, md)
  }
  
  
  
  /**
   * The adapter as defined in the TSML for reading the Dataset.
   */
  lazy val adapter: Adapter = Adapter(model, tsml.dataset.getAdapterAttributes)
  
  /**
   * Singleton instance of Dataset.
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
    if (_dataset.isEmpty) _dataset = adapter.getDataset(operations)
    else _dataset
    _dataset
  }
  
  /**
   * Clean up any resources that the adapter used.
   */
  //TODO: don't construct adapter just to close it
  def close: Unit = adapter.close
  
//TODO: deal with implicit metadata...  
//  /**
//   * Create Metadata from "metadata" elements or Variable element's attributes
//   * in the given Variable XML.
//   */
//  protected def makeMetadata(vml: VariableMl): Metadata = {
//    //Note, not recursive, each Variable's metadata is independent
//    
//    //attributes from the metadata element
//    var atts = vml.getMetadataAttributes
//    
//    if (!atts.contains("name")) vml.getAttribute("id") match {
//      case Some(id) => atts += "name" -> id
//      case None => 
//    }
//    
//    vml.getAttribute("length") match {
//      case Some(l) => atts += "length" -> l
//      case None => 
//    }
//        
//    //internal helper method to add default name for special variable types
//    def addImplicitName(name: String) = {
//      //If the Variable already has a name, add the given name as an alias
//      if (atts.contains("name")) atts.get("alias") match {
//        case Some(a) => atts = atts + ("alias" -> (a+","+name)) //append to list of existing aliases
//        case None => atts = atts + ("alias" -> name) //add alias
//      } 
//      else atts = atts + ("name" -> name) //no 'name' attribute, so use it
//    }
// 
//    if(atts.values.toList.contains("time")) timeUnused = false
//    //Add implicit metadata for "time" and "index" variables.
//    //TODO: consider uniqueness
//    if (timeUnused && vml.label == "time") {addImplicitName("time"); timeUnused = false} //don't add alias if we already have a 'time'
//    if (vml.label == "index") addImplicitName("index")
//    
//
//    Metadata(atts)
//  }
  
}

object TsmlReader2 {
  
  def apply(url: URL): TsmlReader2 = new TsmlReader2(url)

  //def apply(path: String): TsmlReader = new TsmlReader2(path)
  //TODO: replicate TsmlResolver, FileUtil?
  
}







