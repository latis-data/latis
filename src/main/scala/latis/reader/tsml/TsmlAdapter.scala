package latis.reader.tsml

import java.io.File
import java.net.URI
import java.net.URL

import scala.Option.option2Iterable
import scala.collection.Map
import scala.collection.Seq
import scala.collection.immutable
import scala.collection.mutable

import latis.data.Data
import latis.data.seq.DataSeq
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.metadata.Metadata
import latis.ops.MathExpressionDerivation
import latis.ops.Operation
import latis.ops.Projection
import latis.ops.RenameOperation
import latis.ops.UnitConversion
import latis.ops.filter.Selection
import latis.reader.tsml.ml.FunctionMl
import latis.reader.tsml.ml.ScalarMl
import latis.reader.tsml.ml.TimeMl
import latis.reader.tsml.ml.Tsml
import latis.reader.tsml.ml.TupleMl
import latis.reader.tsml.ml.VariableMl
import latis.time.Time
import latis.util.DataUtils


/**
 * Base class for Adapters that read a dataset as defined by TSML.
 */
abstract class TsmlAdapter(val tsml: Tsml) {
  
  /**
   * Abstract method to remind subclasses that they need to clean up their resources.
   */
  def close: Unit
  
  
  //---- Adapter properties from TSML -----------------------------------------
  
  /**
   * Store XML attributes for this Adapter definition as a properties Map.
   */
  private val properties: Map[String,String] = tsml.dataset.getAdapterAttributes()

  /**
   * Return Some property value or None if property does not exist.
   */
  def getProperty(name: String): Option[String] = properties.get(name)
  
  /**
   * Return property value or default if property does not exist.
   */
  def getProperty(name: String, default: String): String = getProperty(name) match {
    case Some(v) => v
    case None => default
  }
  
  //---- Original Dataset Construction ----------------------------------------
  // First pass just defines the data model without any data. 
  // It is generally used as a template while building the final Dataset.
  
  /**
   * The original Dataset as defined by the TSML.
   * This will only include Data values that are defined in the TSML.
   */
  private lazy val origDataset: Dataset = makeOrigDataset
  def getOrigDataset = origDataset
  
  /**
   * Keep a list of the original Scalars.
   * Don't include "index" which is just a placeholder for an otherwise undefined domain.
   * The index value does not appear in the original data source. Otherwise, model it as an Integer.
   */
  private lazy val origScalars = origDataset.toSeq.filterNot(_.isInstanceOf[Index])
  def getOrigScalars = origScalars
  
  /**
   * Keep a list of the names of the original Scalars.
   */
  private lazy val origScalarNames = origScalars.map(_.getName)
  def getOrigScalarNames = origScalarNames
  
  /**
   * Construct the data model for this Dataset.
   */
  protected def makeOrigDataset: Dataset = {
    val md = makeMetadata(tsml.dataset)
    val vars = tsml.dataset.getVariableMl.flatMap(makeOrigVariable(_))
    Dataset(vars, md) 
  } 
  
  /**
   * Create Metadata from "metadata" elements or Variable element's attributes
   * in the given Variable XML.
   */
  protected def makeMetadata(vml: VariableMl): Metadata = {
    //Note, not recursive, each Variable's metadata is independent
    
    //Add tsml attributes for the variable element to attributes from the metadata element.
    //TODO: deprecate, reserve vml attributes for config options for the adapter?
    //      if there is a metadata element, add only name from attributes 
    var atts = vml.getAttributes ++ vml.getMetadataAttributes
    
    //internal helper method to add default name for special variable types
    def addImplicitName(name: String) = {
      //If the Variable already has a name, add the given name as an alias
      if (atts.contains("name")) atts.get("alias") match {
        case Some(a) => atts = atts + ("alias" -> (a+","+name)) //append to list of existing aliases
        case None => atts = atts + ("alias" -> name) //add alias
      } 
      else atts = atts + ("name" -> name) //no 'name' attribute, so use it
    }
 
    //Add implicit metadata for "time" and "index" variables.
    //TODO: consider uniqueness
    if (vml.label == "time") addImplicitName("time")
    if (vml.label == "index") addImplicitName("index")

    Metadata(atts)
  }
    
  /**
   * Construct the Variables of the model recursively.
   */
  private def makeOrigVariable(vml: VariableMl): Option[Variable] = {
    //TODO: support Data values defined in tsml
    val md = makeMetadata(vml)
    vml match {
      case tml: TimeMl => Some(Time(tml.getType, md))
      case sml: ScalarMl => Some(Scalar(sml.label, md))
      case tml: TupleMl  => Some(Tuple(tml.variables.flatMap(makeOrigVariable(_)), md))
      case fml: FunctionMl => for (domain <- makeOrigVariable(fml.domain); range <- makeOrigVariable(fml.range)) 
        yield Function(domain, range, md)
    }
  }
  

  //---- Dataset Construction -------------------------------------------------
  
  //TODO: "build" vs "make"? consider scala Builder
  
  /**
   * Hook for subclasses to do something before constructing the Dataset.
   * Note, this happens after the first Dataset construction pass (building model from TSML)
   * but before the second (reading data).
   */
  def init: Unit = {}
  
  /**
   * The final Dataset that this Adapter produces.
   * It will be constructed when it is first requested.
   */
  private lazy val dataset: Dataset = {
    val ods = origDataset //invoke lazy first Dataset construction pass
    
    //give Adapter the opportunity to be responsible for handling Processing Instructions
    val otherOps = piOps.filterNot(handleOperation(_))
    
    init //let subclasses do something before we start thinking about data access.
    
    //Invoke 2nd pass to build (possibly lazy) Data
    val ds = makeDataset(ods)
    
    //Apply PIs that the adapter didn't handle.
    //Reverse because foldRight applies them in reverse order.
    otherOps.reverse.foldRight(ds)(_(_)) 
  }
  
  /**
   * Public accessor to get the Dataset served by this Adapter.
   */
  def getDataset: Dataset = dataset
  
  /**
   * Return the Dataset with the given sequence of operations applied.
   * It is likely that these operations will be applied lazily and 
   * only invoked as the client iterates on the Dataset.
   */
  def getDataset(ops: Seq[Operation]): Dataset = {
    //TODO: consider implications of calling twice
    
    //Give the adapter the opportunity to handle operations.
    val otherOps = ops.filterNot(handleOperation(_))
    
    //Apply operations that the adapter didn't handle.
    //Reverse because foldRight applies them in reverse order.
    //This should be the first use of the lazy 'dataset' so it may trigger its final construction.
    otherOps.reverse.foldRight(getDataset)(_(_))
  }
  
  /**
   * Initiate construction of the final Dataset. 
   * This will be triggered the the client requests the Dataset.
   */
  protected def makeDataset(ds: Dataset): Dataset = {
    val vars = ds.getVariables.flatMap(makeVariable(_))
    Dataset(vars, ds.getMetadata) 
  } 
  
  /**
   * Build the Variables for the final Dataset recursively using the model from the first pass as a template.
   * These steps are broken into methods for each Variable type so subclasses can more easily override behavior.
   */
  protected def makeVariable(variable: Variable): Option[Variable] = variable match {
    case scalar:   Scalar   => makeScalar(scalar)
    case sample:   Sample   => makeSample(sample)
    case tuple:    Tuple    => makeTuple(tuple)
    case function: Function => makeFunction(function)
  }
  
  /**
   * Build a Scalar from the original model by adding Data.
   * This will look to see if data for this variable has been cached.
   * Note, this will not be called for Scalars within a Function when using the default makeFunction.
   */
  protected def makeScalar(scalar: Scalar): Option[Scalar] = getCachedData(scalar.getName) match {
    case Some(ds) => Some(Scalar(scalar.getMetadata, ds(0))) //Note, not designed for scalars within Functions. 
    case None => Some(scalar) //no-op, //TODO: might as well throw an error since this means no data is defined?
  }
  
  /**
   * Build a Sample Variable from the domain and range components.
   * This will replace the domain with an Index as a placeholder if it is needed.
   */
  protected def makeSample(sample: Sample): Option[Sample] = {
    val odomain = makeVariable(sample.domain)
    val orange  = makeVariable(sample.range)
    (odomain, orange) match {
      case (Some(d), Some(r)) => Some(Sample(d,r))
      case (None, Some(r))    => Some(Sample(Index(), r)) //no domain, so replace with Index. 
      case (Some(d), None)    => Some(Sample(Index(), d)) //no range, so make domain the range of an index function
      case (None, None)       => None //nothing projected
    }
  }
  
  /**
   * Build a Tuple.
   */
  protected def makeTuple(tuple: Tuple): Option[Tuple] = {
    val md = tuple.getMetadata
    val vars = tuple.getVariables.flatMap(makeVariable(_)) 
    vars.length match {
      case 0 => None
      case n => Some(Tuple(vars, md))
      //TODO: make scalar if only one variable? reduce
    }
  }
  
  /**
   * Build a Function.
   */
  protected def makeFunction(f: Function): Option[Function] = {    
    val dataMap = getCache //immutable
    val sampleTemplate = Sample(f.getDomain, f.getRange)
    val data = DataUtils.dataMapToSampledData(dataMap, sampleTemplate)
    Some(Function(f.getDomain, f.getRange, f.getMetadata, data=data))
    //TODO: make sure function md has length?
  }
  
  /**
   * Hook for subclasses to apply operations during data access
   * to reduce data volumes. (e.g. query constraints to database or web service)
   * Return true if it will be handled. Otherwise, it will be applied
   * to the Dataset by the TsmlAdapter class.
   * The default behavior is for the Adapter subclass to handle no operations.
   */
  def handleOperation(op: Operation): Boolean = false 
  
  //---- Caching --------------------------------------------------------------
  //TODO: consider mutability issues
  //TODO: consider caching in SampledData
  
  /**
   * Cache the Data as a Map from the Variable name to a sequence of Data records, 
   * one per sample of the outer Function.
   */
  private val dataCache = mutable.Map[String, DataSeq]()
  
  def getCache: immutable.Map[String, DataSeq] = dataCache.toMap
  
  /**
   * Is the cache empty.
   */
  def cacheIsEmpty: Boolean = dataCache.isEmpty
  
  /**
   * Add Data to the cache.
   * Note, this will replace any data cached for a given variable name.
   */
  protected def cache(dataMap: Map[String, DataSeq]) = dataCache ++= dataMap
  
  /**
   * Replace data for given variable.
   */
  protected def cache(variableName: String, data: Data) = {
    //make sure data is Iterable
    val d: DataSeq = data match {
      case idata: DataSeq => idata
      case _ => DataSeq(data)
    }
    dataCache += (variableName -> d)
  }
  
  /**
   * Append data to given variable.
   */
  protected def appendToCache(variableName: String, data: Data) {
    getCachedData(variableName) match {
      case Some(d) => cache(variableName, d append data) //Note: error if wrong size
      case None => cache(variableName, data) //first sample
    }
  }
  
  //TODO: performance note: parameter with 150100 samples ended up with an ArrayBuffer with 262144 samples
  
  /**
   * Get the Data that has been cached for the given variable.
   */
  def getCachedData(variableName: String): Option[DataSeq] = dataCache.get(variableName)
  //TODO: if None throw new Error("No data found in cache for Variable: " + variableName)? or return empty Data?
  
  //---------------------------------------------------------------------------
  
  /**
   * Get the TSML Processing Instructions as a Seq of Operations.
   */
  def piOps: Seq[Operation] = {
    //TODO: consider order
    //TODO: add other PI types? rename,...
    val projections = tsml.getProcessingInstructions("project").map(Projection(_)) 
    val selections  = tsml.getProcessingInstructions("select").map(Selection(_))
    //Unit conversions: "convert vname units"
    val conversions = tsml.getProcessingInstructions("convert").map(s => {
      val index = s.indexOf(" ")
      val vname = s.substring(0, index).trim
      val units = s.substring(index).trim
      UnitConversion(vname, units)
    })
    
    val renames = tsml.getProcessingInstructions("rename").map(RenameOperation(_)) 
    val derivations = tsml.getProcessingInstructions("derived").map(MathExpressionDerivation(_))
    
    projections ++ selections ++ conversions ++ renames ++ derivations
  }
  
  /**
   * Find a Selection among the given Operations for the given variable name.
   */
  def findSelection(ops: Seq[Operation], name: String): Option[Selection] = {
    ops.filter(_.isInstanceOf[Selection]).map(_.asInstanceOf[Selection]).find(_.vname == name)
  }
  //TODO: move to util? ByName and ByType?

  
  /**
   * Get the URL of the data source from this adapter's definition.
   * This will come from the adapter's 'location' attribute.
   * It will try to resolve relative paths by looking in the classpath
   * then looking in the current working directory.
   */
  def getUrl: URL = {
    //Note, can't be relative to the tsml file since we only have xml here. Tsml could be from any source.
    properties.get("location") match {
      case Some(loc) => {
        val uri = new URI(loc)
        if (uri.isAbsolute) uri.toURL //starts with "scheme:...", note this could be file, http, ...
        else if (loc.startsWith(File.separator)) new URL("file:" + loc) //absolute path
        else getClass.getResource("/"+loc) match { //relative path: try looking in the classpath
          case url: URL => url
          case null => new URL("file:" + scala.util.Properties.userDir + File.separator + loc) //relative to current working directory
        }
      }
      case None => throw new RuntimeException("No 'location' attribute in TSML adapter definition.")
    }
  }
  
  
}

//=============================================================================

object TsmlAdapter {
  
  /**
   * Construct an instance of a TsmlAdapter as defined in the TSML.
   */
  def apply(tsml: Tsml): TsmlAdapter = {
    val atts = tsml.dataset.getAdapterAttributes()
    val class_name = atts("class")
    try {
      val cls = Class.forName(class_name)
      val ctor = cls.getConstructor(tsml.getClass())
      ctor.newInstance(tsml).asInstanceOf[TsmlAdapter]
      //TODO: call init? otherwise nothing happens till getDataset, but often need ops that come with getDataset(ops)
    } catch {
      case e: Exception => {
        //e.printStackTrace()
        throw new Error("Failed to construct Adapter: " + class_name, e)
      }
    }
  }
  
}

