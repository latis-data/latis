package latis.data.set

import latis.data.Data
import latis.data.IterableData

/**
 * Base class for Data representing SampledFunction domain values.
 */
abstract class DomainSet extends IterableData {

  def apply(index: Int): Data
  
  def indexOf(data: Data): Int
  
  def recordSize: Int
  
  def iterator: Iterator[Data]
  
}

object DomainSet {
  
  //temporary hack
  def apply(data: IterableData): DomainSet = new DomainSet {
    def apply(index: Int): Data = ???
    def indexOf(data: Data): Int = ???
   
    def recordSize: Int = data.recordSize
    def iterator: Iterator[Data] = data.iterator
  }
  
}
