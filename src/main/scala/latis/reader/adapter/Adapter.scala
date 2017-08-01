package latis.reader.adapter

import latis.dm._

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
import latis.dm.Model
import latis.dm.ProcessingInstruction
import latis.dm.VariableType
import latis.dm.TupleType
import latis.dm.ScalarType
import java.nio.ByteBuffer
import latis.util.iterator.PeekIterator


/**
 * Base class for Adapters that read a dataset as defined by a Model.
 */
abstract class Adapter(model: Model, properties: Map[String, String]) extends LazyLogging {
  
  /**
   * Abstract method to remind subclasses that they need 
   * to clean up their resources.
   */
  def close: Unit
  
  
  def handlePI(pi: ProcessingInstruction): Boolean = false
    
  /**
   * Hook for subclasses to apply operations during data access
   * to reduce data volumes. (e.g. query constraints to database or web service)
   * Return true if it will be handled. Otherwise, it will be applied
   * to the Dataset by the Adapter class.
   * The default behavior is for the Adapter subclass to handle no operations.
   */
  def handleOperation(op: Operation): Boolean = false
  
  /**
   * Main entry point for Reader to request the Dataset.
   */
  def getDataset(operations: Seq[Operation]): Dataset = {
    // Allow subclasses to handle ProcessingInstructions.
    val otherPIs: Seq[ProcessingInstruction] = model.pis.filterNot(handlePI(_))
    
    // Allow subclasses to handle user Operations.
    val otherOps: Seq[Operation] = operations.filterNot(handleOperation(_))
      
    // Construct the Dataset
    val dataset = makeDataset(model)

    // Apply remaining processing instructions and user Operations.
    val piOps = otherPIs.map(pi => Operation(pi.name, pi.args.split(',').map(_.trim)))
    //TODO: deal with PIs targeted for specific var 
    (piOps ++ otherOps).foldLeft(dataset)((ds, op) => op(ds))
    //TODO: compose (and optimize) Operations (as functions V => V) then apply to dataset
  }
  
  /**
   * Traverse the Model and construct the Dataset.
   */
  def makeDataset(model: Model): Dataset = makeVariable(model.variable) match {
    case Some(v) => Dataset(v, model.metadata)
    case None    => Dataset(null, model.metadata)
  }
  
  //---- Construct Variables --------------------------------------------------
  
  /**
   * Build the Variables for the final Dataset recursively using the model.
   * These steps are broken into methods for each Variable type 
   * so subclasses can more easily override behavior.
   */
  protected def makeVariable(vtype: VariableType): Option[Variable] = vtype match {
    case scalar:   ScalarType   => makeScalar(scalar)
    case tuple:    TupleType    => makeTuple(tuple)
    case function: FunctionType => makeFunction(function)
  }
  
  /**
   * Build a Scalar from the model by adding Data from the cache.
   */
  protected def makeScalar(scalar: ScalarType): Option[Scalar] = {
    //getCachedData(scalar.id).map(Scalar(scalar, _))
    //TODO: error if None? no data in cache
    getCachedData(scalar.id) match {
      case Some(bb) => 
        //TODO: error if no bytes remaining, instead of the dreaded buffer underflow
// Hack for nested Function domain: If we are at the end of a buffer, rewind it
if (scalar.hasName("wavelength") && ! bb.hasRemaining) bb.rewind
        Option(Scalar(scalar, bb))
      case None => 
        if (scalar.getType == "index") Option(Scalar(scalar, null))
        else throw new RuntimeException(s"No data found for scalar: $scalar")
    }
  }

  /**
   * Build a Tuple.
   */
  protected def makeTuple(tuple: TupleType): Option[Tuple] = {
    val md = tuple.metadata
    val vars = tuple.variables.flatMap(makeVariable(_)) 
    vars.length match {
      case 0 => None
      case n => Some(Tuple(vars, md))
      //TODO: if only one variable, don't wrap in Tuple? name space issues
    }
  }
  
  /**
   * Build a Function.
   * This approach assumes that the Adapter subclass has put Data
   * into the cache. 
   * The preferred IterativeAdapter2 overrides this.
   */
  protected def makeFunction(f: FunctionType): Option[Function] = {

    val samples = new PeekIterator[Sample]() {
      private var index = -1
      private val n = f.metadata.get("length") match {
        case Some(s) => s.toInt
        case None    => -1 //unlimited
      }
      
      def getNext: Sample = {
        index += 1
        if (n >= 0 && index >= n) null
        else {
          val os = for {
            d <- makeVariable(f.domain);
            r <- makeVariable(f.codomain)
          } yield Sample(d, r) 

          //TODO: need makeSample to deal with Index logic?
          os match {
            case Some(s) => s
            case None => getNext //skip bad sample
          }
        }
      }
    }
    
    val smp = samples.peek
    Option(SampledFunction(smp.domain, smp.range, samples, f.metadata))
  }
  
  //---- Property Methods -----------------------------------------------------
  
  def getProperty(name: String): Option[String] = properties.get(name)
  
  def getProperty(name: String, default: String): String = properties.getOrElse(name, default)
  
  //---- Caching --------------------------------------------------------------
  
  // Use id in dataCache since name can be changed.
  
  /**
   * Cache the Data as a Map from the Variable name to a ByteBuffer.
   */
  private val dataCache = mutable.Map[String, ByteBuffer]()

  
  //def getCache: immutable.Map[String, DataSeq] = dataCache.toMap
  
  /**
   * Is the cache empty.
   */
  //def cacheIsEmpty: Boolean = dataCache.isEmpty
  
  /**
   * Add Data to the cache as a ByteBuffer.
   * Note, this will replace any data cached for a given variable name.
   */
  //TODO: make sure BB is rewound...
  protected def cache(dataMap: Map[String, Data]): Unit = {
    dataCache ++= dataMap.map(p => (p._1, p._2.getByteBuffer))
  }
  
  /**
   * Replace data for given variable.
   */
  protected def cache(variableName: String, data: Data): Unit = {
    cache(Map(variableName -> data))
  }
  
  /**
   * Get the Data that has been cached for the given variable.
   */
  def getCachedData(variableName: String): Option[ByteBuffer] = {
    dataCache.get(variableName)
  }
  //TODO: if None throw new Error("No data found in cache for Variable: " + variableName)? or return empty Data?

  protected def clearCache: Unit = dataCache.clear
  
  //---------------------------------------------------------------------------

  
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

object Adapter {
  
  /**
   * Construct an instance of a Adapter with the given properties
   * that will build a Dataset as defined in the Model.
   */
  def apply(model: Model, properties: Map[String, String]): Adapter = {
    properties.get("class") match {
      case Some(class_name) =>
        try {
          val cls = Class.forName(class_name)
          val cs = cls.getConstructors
          val ctor = cs.head
          //val ctor = cls.getConstructor(model.getClass(), properties.getClass)
          ctor.newInstance(model, properties).asInstanceOf[Adapter]
        } catch {
          case e: Exception =>
            throw new Error("Failed to construct Adapter: " + class_name, e)
        }
    }
  }
  
}

