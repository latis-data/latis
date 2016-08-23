package latis.ops

import latis.dm.Dataset
import latis.dm.Variable
import latis.metadata.Metadata
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Function
import latis.dm.Sample

class RenameOperation(val origName: String, val newName: String) extends Operation {
  //TODO: this could use some clean up
  
  //make new Dataset with new metadata with the new name
  override def apply(dataset: Dataset): Dataset = {
    val dsmd = dataset.getMetadata
    
    //assume dataset can not have alias
    val name = dataset.getName
    
    if (name == origName) {
      val md = dsmd + ("name" -> newName)
      Dataset(dataset match { case Dataset(v) => v; case _ => null }, md) 
    } else { //try the kids
      val v = dataset match {
        case Dataset(v) => applyToVariable(v) match {
          case Some(v) => v
          case None => throw new Error("No variable found with name: " + origName)
          //TODO: error or no-op?
        }
        case _ => null
      }
      Dataset(v, dsmd)
    }
  }
  
  override def applyToVariable(variable: Variable): Option[Variable] = {
    val md = variable.getMetadata
    val md2 = if (variable.hasName(origName)) md + ("name" -> newName) else md
    
    //contruct variable with new metadata recurse
    //TODO: avoid reconstructing vars whose name didn't change
    variable match {
      //case s: Scalar => Some(Scalar(md2, s.getData))
      case s: Scalar => {
        if (s.hasName(origName)) Some(s.updatedMetadata("name" -> newName))
        else Some(s)
      }
      case Tuple(vars) => Some(Tuple(vars.flatMap(applyToVariable(_)), md2))
      case f @ Function(samples) => {
        //need to munge domain and range if we are going to map iterator (lazy)
        val d = applyToVariable(f.getDomain).get
        val r = applyToVariable(f.getRange).get
        Some(Function(d, r, samples.flatMap(applyToSample(_)), md2))
      }
    }
  }
  
  /**
   * Override to apply to domain as well as range variables.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    for (d <- applyToVariable(sample.domain); r <- applyToVariable(sample.range)) yield Sample(d,r)
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
