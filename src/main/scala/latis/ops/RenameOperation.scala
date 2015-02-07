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
    val dsmd = dataset.getMetadata
    
    if (dataset.hasName(origName)) {
      val md = changeName(dsmd, newName)
      Dataset(dataset.getVariables, md) 
    } else { //try the kids
      val vars = dataset.getVariables.flatMap(applyToVariable(_))
      Dataset(vars, dsmd)
    }
  }
  
  override def applyToVariable(variable: Variable): Option[Variable] = Some(variable)
//  {
//    //TODO: deal with vars with same name or alias
//    val md = variable.getMetadata
//    val md2 = if (variable.hasName(origName)) changeName(md, newName) else md
//    
//    //recurse
//    variable match {
//      case 
//    }
//  }
  
  private def changeName(md: Metadata, name: String): Metadata = {
    Metadata(md.getProperties + ("name" -> name))
  }
}

object RenameOperation extends OperationFactory {
  
  override def apply(args: Seq[String]) = RenameOperation(args(0), args(1))
  //TODO: error handling
  
  def apply(expression: String): RenameOperation = {
    //assume "name1,name2"
    val ss = expression.split(",")
    new RenameOperation(ss(0), ss(1))
  }
    
  def apply(name1: String, name2: String): RenameOperation = new RenameOperation(name1, name2)
  
  def unapply(renameOp: RenameOperation) = Some(renameOp.origName, renameOp.newName)
}