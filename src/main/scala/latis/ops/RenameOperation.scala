package latis.ops

import latis.dm.Dataset
import latis.dm.Variable
import latis.metadata.Metadata

class RenameOperation(val origName: String, val newName: String) extends Operation {

  /*
   * TODO: apply to any Variable
   */
  
  //make new Dataset with new metadata with the new name
  override def apply(dataset: Dataset): Dataset = {
    if (dataset.hasName(origName)) {
      //TODO: deal with vars with same name or alias
      val md = Metadata(dataset.getMetadata.getProperties + ("name" -> newName))
      Dataset(dataset.getVariables, md) 
    } else dataset
  }
  
  override def applyToVariable(variable: Variable): Option[Variable] = ???
}

object RenameOperation {
  
  def apply(name1: String, name2: String) = new RenameOperation(name1, name2)
  
  def unapply(renameOp: RenameOperation) = Some(renameOp.origName, renameOp.newName)
}