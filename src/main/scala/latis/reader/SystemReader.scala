package latis.reader

import latis.dm.Dataset
import latis.dm._
import latis.metadata.Metadata
import latis.ops.Operation
import latis.dm.Variable

/**
   * Construct a Dataset by querying the JVM.
   *   (free, used, total, percentUsed, max)
   * Where
   *   free:        is the current amount of memory in Mb available
   *   used:        is the amount of memory in Mb computed from total - free.
   *   total:       the current memory in Mb allocated on the heap
   *   percentUsed: the ratio of used / total in percent
   *   max:         the maximum memory in Mb availble to the current JVM (-Xmx config setting)
   */
class SystemReader private() extends DatasetAccessor { 
  
  private val MiB = 1024 * 1024
  private val NAME = "name"
  private val UNITS = "units"
  private val metaData =  Metadata("memoryProperties")
  private lazy val jvm = Runtime.getRuntime
  
  override def getDataset(): Dataset = Dataset(Tuple(getFree, getUsed, getTotal, getPercentUsed, getMax), metaData)
  
  def getDataset(operations: Seq[Operation]): Dataset = {
    val dataset = getDataset
    operations.foldLeft(dataset)((ds,op) => op(ds))
  }
  
  def close: Unit = {}
  
  def getFree: Integer = Integer(Metadata(Map(NAME -> "freeMemory", UNITS -> "MiB")), 
      jvm.freeMemory / MiB)
  
  def getUsed: Integer = Integer(Metadata(Map(NAME -> "usedMemory", UNITS -> "MiB")), 
      (jvm.totalMemory() - jvm.freeMemory) / MiB)
  
  def getTotal: Integer = Integer(Metadata(Map(NAME -> "totalMemory", UNITS -> "MiB")), 
      jvm.totalMemory() / MiB)
  
  def getPercentUsed: Integer = Integer(Metadata(Map(NAME -> "percentUsed", UNITS -> "percent")), 
      100 * (jvm.totalMemory() - jvm.freeMemory) / jvm.totalMemory())
  
  def getMax: Integer = Integer(Metadata(Map(NAME -> "maxMemory", UNITS -> "MiB")),
      jvm.maxMemory() / MiB)
  
}

object SystemReader {
  def apply(): SystemReader = new SystemReader
}

