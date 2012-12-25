package latis.dm

class Index extends Scalar

object Index {
  def apply() = new Index()
  
  def unapply(v: Index): Option[Int] = {
    v.getDataset().getAccessor().getValue(v)
  }
}