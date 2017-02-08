package latis.reader.tsml

import java.io.File
import java.net.URI
import java.net.URL
import java.net.URLDecoder
import java.net.URLEncoder
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
import latis.ops.DomainBinner
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
import latis.ops.filter.Selection
import java.net.MalformedURLException
import latis.util.StringUtils
import scala.collection.mutable.ArrayBuffer
import latis.ops.TimeFormatter
import latis.ops.ReplaceMissingOperation
import latis.ops.Pivot
import latis.ops.TimeTupleToTime
import com.typesafe.scalalogging.LazyLogging


/**
 * Base class for Adapters that read a dataset as defined by TSML.
 */
abstract class TsmlAdapter(val tsml: Tsml) extends LazyLogging {
  
  /**
   * Abstract method to remind subclasses that they need to clean up their resources.
   */
  def close: Unit
  
  
  //---- Adapter properties from TSML -----------------------------------------
  
  /**
   * Store XML attributes for this Adapter definition as a properties Map.
   */
  private var properties: Map[String,String] = tsml.dataset.getAdapterAttributes()

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
  
  /**
   * Allow adapters to manipulate their properties.
   */
  protected def setProperty(name: String, value: String): Unit = {
    properties = properties + (name -> value)
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
  private lazy val origScalars = getOrigDataset match {
    case Dataset(v) => v.toSeq.filterNot(_.isInstanceOf[Index])
    case _ => Seq()
  }
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
    makeOrigVariable(tsml.dataset.getVariableMl) match {
      case Some(v) => Dataset(v, md)
      case None => throw new Error("No variables made for original dataset.")
    }
  } 
  
  var timeUnused = true //variable used to prevent multiple time aliases
  
  /**
   * Create Metadata from "metadata" elements or Variable element's attributes
   * in the given Variable XML.
   */
  protected def makeMetadata(vml: VariableMl): Metadata = {
    //Note, not recursive, each Variable's metadata is independent
    
    //attributes from the metadata element
    var atts = vml.getMetadataAttributes
    
    if (!atts.contains("name")) vml.getAttribute("id") match {
      case Some(id) => atts += "name" -> id
      case None => 
    }
    
    vml.getAttribute("length") match {
      case Some(l) => atts += "length" -> l
      case None => 
    }
        
    //internal helper method to add default name for special variable types
    def addImplicitName(name: String) = {
      //If the Variable already has a name, add the given name as an alias
      if (atts.contains("name")) atts.get("alias") match {
        case Some(a) => atts = atts + ("alias" -> (a+","+name)) //append to list of existing aliases
        case None => atts = atts + ("alias" -> name) //add alias
      } 
      else atts = atts + ("name" -> name) //no 'name' attribute, so use it
    }
 
    if(atts.values.toList.contains("time")) timeUnused = false
    //Add implicit metadata for "time" and "index" variables.
    //TODO: consider uniqueness
    if (timeUnused && vml.label == "time") {addImplicitName("time"); timeUnused = false} //don't add alias if we already have a 'time'
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
    //This should be the first use of the lazy 'dataset' so it may trigger its final construction.
    otherOps.foldLeft(getDataset)((dataset, op) => op(dataset))
  }
  
  /**
   * Initiate construction of the final Dataset. 
   * This will be triggered the the client requests the Dataset.
   */
  protected def makeDataset(ds: Dataset): Dataset = {
    ds match {
      case Dataset(v) => makeVariable(v) match {
        case Some(v) => Dataset(v, ds.getMetadata)
        case None => {
          logger.warn("Empty Dataset created for " + ds.getName)
          Dataset.empty
        }
      }
      case _ => Dataset.empty 
    }
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
  
//  private val dataCache: Cache = {
//    val name = origDataset.getName
//    val manager = CacheManager.getInstance //TODO:close manager?
//    manager.getCacheNames.contains(name) match {
//      case true => manager.getCache(name)
//      case false => {
//        manager.addCache(name)
//        manager.getCache(name)
//      }
//    }
//  }
  
//  def closeCache: Unit = {
//    dataCache.dispose
//    CacheManager.getInstance.shutdown
//  }
  
  def getCache: immutable.Map[String, DataSeq] = dataCache.toMap
  
//  def getCache: immutable.Map[String, DataSeq] = dataCache.getAll(dataCache.getKeys).values.asScala.map(e => e.getObjectKey -> e.getObjectValue).toMap.asInstanceOf[immutable.Map[String, DataSeq]]
  
  /**
   * Is the cache empty.
   */
  def cacheIsEmpty: Boolean = dataCache.isEmpty
  
//  def cacheIsEmpty: Boolean = dataCache.getKeys.isEmpty
  
  /**
   * Add Data to the cache.
   * Note, this will replace any data cached for a given variable name.
   */
  protected def cache(dataMap: Map[String, DataSeq]) = dataCache ++= dataMap
  
//  protected def cache(dataMap: Map[String, DataSeq]) = dataMap.foreach(p => dataCache.put(new Element(p._1, p._2)))
  
  /**
   * Replace data for given variable.
   */
  protected def cache(variableName: String, data: Data): Unit = {
    //make sure data is Iterable
    val d: DataSeq = data match {
      case idata: DataSeq => idata
      case _ => DataSeq(data)
    }
    cache(Map(variableName -> d))
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
  
//  def getCachedData(variableName: String): Option[DataSeq] = {
//    try Some(dataCache.get(variableName).getObjectValue.asInstanceOf[DataSeq])
//    catch {case npe: NullPointerException => None}
//  }
  
  //---------------------------------------------------------------------------
  
  /**
   * Get the TSML Processing Instructions as a Seq of Operations.
   * Ops with multiple input parameters should have those parameters separated by ','
   * in the processing instruction.
   */
  def piOps: Seq[Operation] = {
    tsml.processingInstructions.map(pi => Operation(pi._1, pi._2.split(',').map(_.trim)))
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
      case Some(loc) => StringUtils.getUrl(loc)
      case None => throw new RuntimeException("No 'location' attribute in TSML adapter definition.")
    }
  }
  
  /**
   * Get the "location" attribute of the tsml file as a java.io.File
   * 
   * This is a relatively simple wrapper around getUrl. It's a pretty
   * common operation to get "location" as a java.net.URL and then
   * try to open that url as a file ("file:/foo/bar"). Unfortunately,
   * that's also a pretty buggy process. For example, a path containing spaces
   * like "/opt/my stuff/data" will be transformed to
   * "/opt/my%20stuff/data" because spaces aren't valid inside a
   * URL. However, spaces are valid as a path name, so this
   * transformation causes File creation to fail (FileNotFoundException)
   * in rare (usually system-dependent) scenarios. Therefore, this
   * wrapper method is intended to contain that workaround and
   * any other workarounds needed to convert URLs to Files that
   * we may find in the future.
   */
  def getUrlFile: File = {
    val url = getUrl
    new File(URLDecoder.decode(url.getPath,"utf-8"))
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

