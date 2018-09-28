package latis.ops

import latis.metadata.Metadata
import latis.dm._

class UpdateMetadata(vname: String, update: (String, String)) extends Operation {
  
  //make new Dataset with new metadata with the new name
  override def apply(dataset: Dataset): Dataset = {
    val dsmd = dataset.getMetadata
    
    //assume dataset can not have alias
    val name = dataset.getName
    
    if (name == vname) {
      val md = dsmd + update
      Dataset(dataset match { case Dataset(v) => v; case _ => null }, md) 
    } else { //try the kids
      val v = dataset match {
        case Dataset(v) => applyToVariable(v) match {
          case Some(v) => v
          case None => throw new RuntimeException("No variable found with name: " + vname)
          //TODO: error or no-op?
        }
        case _ => null
      }
      Dataset(v, dsmd)
    }
  }
  
  override def applyToVariable(variable: Variable): Option[Variable] = {
    val md = variable.getMetadata
    val md2 = if (variable.hasName(vname)) md + update else md
    
    //contruct variable with new metadata recurse
    //TODO: avoid reconstructing vars whose name didn't change
    variable match {
      //case s: Scalar => Some(Scalar(md2, s.getData))
      case s: Scalar => {
        if (s.hasName(vname)) Some(s.updatedMetadata(update))
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

object UpdateMetadata extends OperationFactory {
  
  def apply(name: String, kv: (String, String)): UpdateMetadata = new UpdateMetadata(name, kv)
  
  def apply(name: String, field: String, value: String): UpdateMetadata = UpdateMetadata(name, field -> value)
  
  override def apply(args: Seq[String]): UpdateMetadata = UpdateMetadata(args(0), args(1), args(2))
  
}

