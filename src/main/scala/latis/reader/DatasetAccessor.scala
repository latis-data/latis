package latis.reader

import latis.dm.Dataset
import latis.ops.Operation
import latis.util._
import com.typesafe.scalalogging.LazyLogging
import latis.reader.tsml.ml.TsmlResolver
import latis.util.ReflectionUtils
import latis.reader.tsml.TsmlReader
import latis.util.CacheManager
import java.net.URL

/**
 * Base class that provide data access for a Dataset.
 * A DatasetAccessor is responsible for constructing a Dataset
 * and returning data for the Variables it contains.
 */
abstract class DatasetAccessor {

  /**
   * Return the Dataset that this accessor is responsible for.
   * This may involve considerable construction.
   */
  def getDataset(): Dataset = getDataset(Seq[Operation]())
  
  /**
   * Return the Dataset with the given Operations applied to it.
   */
  def getDataset(operations: Seq[Operation]): Dataset
  
  /**
   * Get the URL of the data source from this adapter's definition.
   * This will come from the adapter's 'location' attribute.
   * It will try to resolve relative paths by looking in the classpath
   * then looking in the current working directory.
   */
  def getUrl: URL = {
    //TODO: borrowed from TsmlAdapter, can we share this impl?
    //Note, can't be relative to the tsml file since we only have xml here. Tsml could be from any source.
    properties.get("location") match {
      case Some(loc) => StringUtils.getUrl(loc)
      case None => throw new RuntimeException("No 'location' defined for dataset.")
    }
  }
  
  /**
   * Release any resources that this accessor acquired.
   */
  def close()
  
  //---- reader properties from latis.properties ------------------------------
  
  /**
   * Store latis.properties as a properties Map.
   */
  private var properties: Map[String,String] = Map[String,String]()

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

}

object DatasetAccessor extends LazyLogging {
  
  /**
   * Return a Dataset with the given name and optional Seq of Operations applied to it.
   * Memoize the data within the Dataset and release the source.
   */
  def readDataset(datasetName: String, ops: Seq[Operation] = Seq.empty): Dataset = {
    //TODO: refactor to use FP idioms
    var reader: DatasetAccessor = null
    var ds: Dataset = null
    try {
      reader = DatasetAccessor.fromName(datasetName)
      ds = reader.getDataset(ops).force //memoize so we can release resources
    } catch {
      case e: Exception => throw new RuntimeException(s"Failed to read dataset: $datasetName", e)
    } finally {
      try {reader.close} catch {case _: Exception =>} //close if we can but don't complain
    }
    ds
  }
  
  /**
   * Construct a DatasetAccessor given the name of a Dataset.
   * This will first look for a "reader.dsName.class" property.
   * If not found, it will delegate to the TsmlResolver.
   */
  def fromName(datasetName: String): DatasetAccessor = {
    //See if the dataset is cached.
    val reader = CacheManager.getDataset(datasetName) match {
      case Some(ds) => DatasetAccessor(ds)
      case None => {
        //Look for a matching "reader" property.
        LatisProperties.get(s"reader.${datasetName}.class") match {
          case Some(s) => ReflectionUtils.constructClassByName(s).asInstanceOf[DatasetAccessor]
          case None => {
            //Try TsmlResolver
            val tsml = TsmlResolver.fromName(datasetName)
            logger.debug("Reading dataset from TSML")
            TsmlReader(tsml)
          }
        }
      }
    }
    
    //Add properties from latis.properties
    reader.properties = {
      LatisProperties.getPropertiesWithRoot("reader." + datasetName) + ("name" -> datasetName)
    }
    reader
  }
  
  /**
   * Implement a DatasetAccessor that simply wraps a Dataset.
   */
  def apply(dataset: Dataset): DatasetAccessor = new DatasetAccessor() {
    //TODO: make sure it is memoized, immutable
    def getDataset(operations: Seq[Operation]): Dataset = {
      operations.foldLeft(dataset)((ds, op) => op(ds))
    }
    
    def close {}
  }
}
