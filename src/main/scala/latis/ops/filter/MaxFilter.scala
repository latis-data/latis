package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Variable
import latis.dm.Tuple
import latis._
import latis.metadata.Metadata
import scala.collection.mutable.ArrayBuffer

/**
 * Return the sample containing the max Scalar of any outer Function in the Dataset.
 */
class MaxFilter(name: String) extends Filter {
  
  //While you're at it, if you don't find Scalar of "name," why not return that empty function?
  //(That logic will obviously be handled in applyToFunction. Just set this to any old Scalar.
  var currentMax = Scalar("0.0") //Wrong. This should probably be first Scalar of name "name"
  var keepSamples = ArrayBuffer[Sample]()
  
  override def applyToFunction(function: Function) = {
    //Apply Operation to every sample from the iterator
    function.iterator.foreach(applyToSample(_))
    
    //change length of the Function in metadata
    val md = Metadata(function.getMetadata.getProperties + ("length" -> keepSamples.length.toString))
    
    //make the new function with the updated metadata
    keepSamples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
      case _ => Some(Function(keepSamples, md))
    }
  }
  
  /**
   * Apply Operation to a Sample
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    
      //add if statement here to check if "name" exists in sample
      //if it doesn't, make that empty function here!
      val vars = sample.range.toSeq  //This shouldn't exclude domain like now. 
      var containsName = false       //Well really, all of this code shouldn't exist.
      for (variable <- vars) {       //There's definitely a better way to do this.
        if (variable.getName == name)
          containsName = true
      }
      if (!containsName) {
        return None
      }
   
 
      val x = sample.getVariables.map(applyToVariable(_)) 
      x.find(_.isEmpty) match { //Watch this. Assuming it throws away bad variables, thus bad samples.
        case None => {          
          val s = Sample(sample.domain, sample.range)
          keepSamples += s
          Some(s)
        }
        case Some(_) => None    //found an invalid variable, exclude the entire sample
      }
  }
  
  /**
   * Apply Operation to a Variable, depending on type
   */
  override def applyToVariable(variable: Variable): Option[Variable] = variable match {
    case scalar: Scalar     =>  applyToScalar(scalar)
    case sample: Sample     =>  applyToSample(sample)
    case tuple: Tuple       =>  applyToTuple(tuple)
    case function: Function => applyToFunction(function)
  }
  
  /**
   * Apply Operation to a Tuple
   */
  override def applyToTuple(tuple: Tuple): Option[Tuple] = {
    val x = tuple.getVariables.map(applyToVariable(_))
    x.find(_.isEmpty) match{
      case Some(_) => None //found an invalid variable, exclude the entire tuple
      case None => Some(Tuple(x.map(_.get), tuple.getMetadata))
    }
  }

  /**
   * Apply Operation to a Scalar
   */
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
		//If the filtering causes an exception, log a warning and return None.
		try {
			scalar match {

			  case s: Scalar => if (scalar.hasName(name)) { 
				  val comparison = s.compare(currentMax)
						if (comparison > 0) {
						  //Found a new max value, so initiate grand master plan...
						  keepSamples.clear 
						  currentMax = s
						  Some(scalar) 
					  }
						else if (comparison == 0) {
				      //Keep the sample
				      Some(scalar)
			      }
			      else {
				      //Trash this sample! 
				      None
			      }
			    } else { 
			        Some(scalar) //no-op
			      }
		    }
		  } catch {
		  case e: Exception => {
		   //logger.warn("Selection filter threw an exception: " + e.getMessage)
			  None
		  }
	  }
  }
}

object MaxFilter extends OperationFactory {
  def apply(name: String): MaxFilter = new MaxFilter(name)
}
