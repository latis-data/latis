package latis.dm

/**
 * The basic unit of the LaTiS data model.
 */
abstract class Variable {

  /**
   * The parent of this Variable in the context of a Dataset.
   */
  private[this] var _parent: Variable = null
  
  /**
   * Return the parent Variable of this Variable.
   */
  def getParent() = _parent
  
  /**
   * Tell kids who their parent is when parent is constructed.
   * TODO: restrict to factory method, Tuple and Function construction
   */
  def setParent(parent: Variable) {
    _parent = parent
  }
  
  
  /**
   * Return the Dataset that this Variable belongs to.
   * Iterate up the ancestry until we find a Variable that is a Dataset.
   */
  def getDataset(): Dataset = {
    this match {
      case ds: Dataset => ds
      case _ => this.getParent().getDataset()
    }
  }
}