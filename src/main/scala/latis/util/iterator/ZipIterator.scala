package latis.util.iterator
  
/**
* Synchronizes a Seq[Iterator] into an Iterator[Seq]
*/
class ZipIterator[T>:Null](its: Seq[Iterator[T]]) extends PeekIterator[Seq[T]] {
  def getNext: Seq[T] = {
    val nexts = its.map(it => if(it.hasNext) it.next else null)
    if(nexts.forall(_!=null)) nexts else null
  }
}
