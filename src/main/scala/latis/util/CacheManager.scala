package latis.util

import scala.collection._
import latis.dm.Dataset

/**
 * A Singleton to hold instances of a Dataset in a Map with
 * the dataset name as the key.
 * This will likely evolve to use more sophisticated caching mechanisms.
 */
class CacheManager {
  
  /**
   * Maintain the Datasets in a Map.
   */
  private val cache = mutable.Map[String,Dataset]()
}


/**
 * Companion object where we encapsulate the single instance and
 * expose the public methods.
 */
object CacheManager {
  
  /**
   * Singleton instance of the CacheManager.
   */
  private lazy val instance = new CacheManager()
  
  /**
   * Add a dataset to the cache.
   * The Dataset will be memoized to ensure that it contains all of its
   * data so it is no longer coupled to its source.
   * This will return the cached dataset since the original may be spent
   * due to TraversableOnce issues.
   * It is expected that the Dataset has Metadata that defines the name.
   */
  def cacheDataset(dataset: Dataset): Dataset = {
    //Add creation time to metadata for cache invalidation
    val md = dataset.getMetadata + ("creation_time" -> System.currentTimeMillis.toString)
    //Make sure dataset is memoized (all the Data loaded)
    dataset.force match {
      case Dataset(v) =>
        val ds = Dataset(v, md) //update metadata
        instance.cache += dataset.getName -> ds //add to cache
        ds
      case _ => dataset //empty Dataset, don't cache
    }
  }
  
  /**
   * Optionally get the Dataset with the given name.
   */
  def getDataset(name: String): Option[Dataset] = {
    instance.cache.get(name)
  }
  
  /**
   * Return an immutable Map of dataset name to Dataset instance.
   */
  def getDatasets: immutable.Map[String,Dataset] = instance.cache.toMap  //make immutable

  /**
   * Remove all entries from the cache.
   */
  def clear: Unit = instance.cache.clear
  
  /**
   * Remove a single dataset from the cache.
   */
  def removeDataset(name: String): Option[Dataset] = instance.cache.remove(name)
}
