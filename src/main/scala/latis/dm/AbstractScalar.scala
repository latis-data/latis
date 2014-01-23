package latis.dm

import latis.data._
import latis.metadata._

abstract class AbstractScalar(metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractVariable(metadata, data) with Scalar {

}