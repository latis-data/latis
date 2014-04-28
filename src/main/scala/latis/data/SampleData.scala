package latis.data

/**
 * Data for a Function Sample that keeps the domain and range data separate.
 */
case class SampleData(val domainData: Data, val rangeData: Data) extends Data {

  def getByteBuffer = (domainData concat rangeData).getByteBuffer
  
  def isEmpty = rangeData.isEmpty
  //TODO: also test if domain is empty? or default to index?
  
  def size = domainData.size + rangeData.size
}

