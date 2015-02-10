package latis.ops

import latis.dm._

class Evaluation extends Operation {
  
  /*
   * TODO: need to know which function to apply this to, by domain var name?
   * 
   * function.apply(arg, strategy)
   *   strategy optional, otherwise use the one tied to the function?
   *   but can only apply operation to dataset
   *   
   */
  
  override def applyToFunction(function: Function): Option[Variable] = {
  
    
    ???
  }
}