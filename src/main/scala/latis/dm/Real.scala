package latis.dm

class Real extends Scalar {

}

object Real {
  def apply() = new Real()
  
  
  def unapply(v: Real): Option[Double] = {
    v.getDataset().getAccessor().getValue(v)
  }
}