package latis.dm

import latis.data.Data
import latis.data.EmptyData
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata

import scala.collection.Seq
import scala.collection.immutable


class AbstractTuple(variables: immutable.Seq[Variable], metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractVariable(metadata, data) with Tuple {
  
  def getVariables: Seq[Variable] = variables
}
