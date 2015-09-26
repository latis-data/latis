package latis.reader

import latis.dm.Dataset
import latis.ops.Operation
import latis.util.LatisProperties
import com.typesafe.scalalogging.LazyLogging
import latis.reader.tsml.ml.TsmlResolver
import latis.util.ReflectionUtils
import latis.reader.tsml.TsmlReader

/**
 * Trait that provide data access for a Dataset.
 * A DatasetAccessor is responsible for constructing a Dataset
 * and returning data for the Variables it contains.
 */
trait DatasetAccessor {

  /**
   * Return the Dataset that this accessor is responsible for.
   * This may involve considerable construction.
   */
  def getDataset(): Dataset
  
  /**
   * Return the Dataset with the given Operations applied to it.
   */
  def getDataset(operations: Seq[Operation]): Dataset
  
  
  /**
   * Release any resources that this accessor acquired.
   */
  def close()
}

object DatasetAccessor extends LazyLogging {
  
  /**
   * Construct a DatasetAccessor given the name of a Dataset.
   * This will first look for a "reader.dsName.class" property.
   * If not found, it will delegate to the TsmlResolver.
   */
  def fromName(datasetName: String): DatasetAccessor = {
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