package latis.reader.adapter

import latis.dm._
import latis.ops._
import latis.data.Data
import scala.collection._
import java.nio.ByteBuffer
import latis.util.DataUtils
import latis.util.StringUtils
import java.net.URL

abstract class Adapter3(model: Dataset3, config: AdapterConfig) {
  
  /**
   * Abstract method to remind subclasses that they need 
   * to clean up their resources.
   */
  def close: Unit
  
  /**
   * Offer an Adapter the chance to handle a processing instruction.
   * It should return true to prevent the default application.
   */
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
   * Main entry point for the Reader to request the Dataset
   * with the given sequence of operation applied.
   */
  def getDataset(operations: Seq[Operation]): Dataset3 = {
    // Allow subclasses to handle ProcessingInstructions.
    val unhandledPIs: Seq[ProcessingInstruction] = config.processingInstructions.filterNot(handlePI(_))
    
    // Allow subclasses to handle user Operations.
    val unhandledOps: Seq[Operation] = operations.filterNot(handleOperation(_))
      
    // Hook for subclasses to do some things before making the Dataset.
    //preMakeDataset
    
    // Construct the Dataset based on the Model.
    val dataset = makeDataset(model)

    //TODO: need Operation3
//    // Apply remaining processing instructions and user Operations.
//    val piOps = unhandledPIs.map(pi => Operation(pi.name, pi.args.split(',').map(_.trim)))
//    //TODO: deal with PIs targeted for specific var 
//    (piOps ++ unhandledOps).foldLeft(dataset)((ds, op) => op(ds))
//    //TODO: compose (and optimize) Operations (as functions V => V) then apply to dataset
    dataset
  }
  
  //---- Construct Dataset ----------------------------------------------------

  def makeDataset(model: Dataset3): Dataset3 = makeVariable(model.variable) match {
    case Some(v) => Dataset3(model.id, v, model.metadata)
    case None    => Dataset3(model.id, null, model.metadata)
  }
  
  def makeVariable(variable: Variable3): Option[Variable3] = variable match {
    case s: Scalar3   => makeScalar(s)
    case t: Tuple3    => makeTuple(t)
    case f: Function3 => makeFunction(f)
  }
  
  def makeScalar(scalar: Scalar3): Option[Scalar3] = Option {
    val f = () => getNextData(scalar)
    scalar.copy(get = f)
  }
  
  def makeTuple(tuple: Tuple3): Option[Tuple3] = Option {
    val vars = tuple match {
      //TODO: reduce tuple of 0 or 1?
      case Tuple3(_,_,vs) => vs.flatMap(makeVariable(_))
    }
    //tuple.copy(variables = vars: _*) //TODO: varargs breaks copy?
    Tuple3(tuple.id, tuple.metadata, vars)
  }
  
  /**
   * This will make a SampledFunction that will iterate over samples.
   * This will use the "length" specification in the Function metadata.
   * If length is not defined, it will continue until interrupted.
   * This could be a limited "take" operation or buffer underflow 
   * if the data runs out. 
   * Subclasses may override this to add their own EOF logic.
   * Note that we reuse the same sample for each iteration to avoid so 
   * many objects. The "get" function in Scalar is responsible for maintaining
   * a pointer into the data.
   * It is generally preferred to make subclasses of the IterativeAdapter
   * which overrides makeFunction.
   */
  def makeFunction(function: Function3): Option[Function3] = Option {
    val sample = makeSample(function.domain, function.codomain)
    //TODO: if sample is None
    
    new Function3(function.id, function.metadata, function.domain, function.codomain)
    with SampledFunction3 {
      val length = function.getMetadata("length") match {
        case Some(n) => n.toLong //TODO: handle error
        case None    => ??? //TODO: error for now while testing java.lang.Long.MAX_VALUE
      }
      def iterator: Iterator[(Variable3, Variable3)] = {
        (0l until length).iterator.flatMap(i => sample)
        //TODO: Iterator[Option[A]].flatMap will auto drop Nones.
      }
    }
  }

  /**
   * Given the use of the cache, we only need to make one sample.
   */
  def makeSample(domain: Variable3, codomain: Variable3): Option[(Variable3, Variable3)] = {
    //TODO: if only one is None, replace with Index
    for {
      domain   <- makeVariable(domain)
      codomain <- makeVariable(codomain)
    } yield (domain, codomain)
  }

  
  //---- Caching --------------------------------------------------------------
  
  /**
   * Cache the Data as a Map from the Variable name to a ByteBuffer.
   * The key should be the "id" of the Variable, not necessarily the "name".
   */
  private val dataCache = mutable.Map[String, ByteBuffer]()
  
  /**
   * Add Data to the cache as a ByteBuffer.
   * Note, this will replace any data cached for a given variable name.
   */
  //TODO: make sure BB is rewound... or is that the Data constructor's responsibility?
  protected def cache(dataMap: Map[String, Data]): Unit =
    dataCache ++= dataMap.map(p => (p._1, p._2.getByteBuffer))
  
  /**
   * Replace data for given variable.
   */
  protected def cache(vid: String, data: Data): Unit =
    cache(Map(vid -> data))
  
  /**
   * Get the Data that has been cached for the given variable.
   */
  def getCachedData(vid: String): Option[ByteBuffer] =
    dataCache.get(vid)
  //TODO: if None throw new Error("No data found in cache for Variable: " + variableName)? or return empty Data?

  protected def clearCache: Unit = dataCache.clear
  
  /**
   * Return the next chunk of Bytes as Data for the given Scalar.
   */
  def getNextData(scalar: Scalar3): Data = {
    getCachedData(scalar.id) match {
      case Some(bb) => 
        //TODO: error if no bytes remaining, instead of the dreaded buffer underflow
// Hack for nested Function domain: If we are at the end of a buffer, rewind it
if (scalar.hasName("wavelength") && ! bb.hasRemaining) bb.rewind
        scalar.getType match {
          case "integer" => Data(bb.getLong)
          case "real"    => Data(bb.getDouble)
          case "text"    => 
            val n = scalar.getMetadata("length") match {
              case Some(l) => l.toInt //TODO: handle error
              case None => Text.DEFAULT_LENGTH //4 chars (8 bytes)
            }
            Data(StringUtils.byteBufferToString(bb, n))
        }
      case None => 
        if (scalar.getType == "index") Data.empty
        else throw new RuntimeException(s"No data found for scalar: $scalar")
    }
  }
  
  
  //---- Property Methods -----------------------------------------------------
  
  def getProperty(name: String): Option[String] = config.properties.get(name)
  
  def getProperty(name: String, default: String): String = config.properties.getOrElse(name, default)
  
  //---------------------------------------------------------------------------
  
  /**
   * Get the URL of the data source from this adapter's configuration.
   */
  def getUrl: URL = config.getUrl
}

//=============================================================================

object Adapter3 {
  
  /**
   * Construct an instance of a Adapter with the given properties
   * that will build a Dataset as defined in the Model.
   */
  def apply(model: Dataset3, config: AdapterConfig): Adapter3 = {
    config.properties.get("class") match {
      case Some(class_name) =>
        try {
          val cls = Class.forName(class_name)
          val cs = cls.getConstructors
          val ctor = cs.head
          //val ctor = cls.getConstructor(model.getClass(), properties.getClass)
          ctor.newInstance(model, config).asInstanceOf[Adapter3]
        } catch {
          case e: Exception =>
            throw new Error("Failed to construct Adapter: " + class_name, e)
        }
    }
  }
  
}

