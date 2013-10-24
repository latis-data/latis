package latis.dm

import latis.data._
import latis.metadata._

abstract class AbstractScalar(metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends Variable2(metadata, data) with Scalar {

}