package latis.ops.filter

import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.LazyLogging

import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.metadata.Metadata
import latis.ops.OperationFactory

/**
 * Return the sample containing the min Scalar of any outer Function in the Dataset.
 */
class MinFilter(name: String) extends Filter with LazyLogging {

  var currentMin: Scalar = null //This gets immediately reset to first Scalar of name "name" 
  var keepSamples = ArrayBuffer[Sample]()
  
  override def applyToFunction(function: Function): Option[Function] = {
    //Set currentMin to the first Scalar of name "name"
    //or return an empty function if no such Scalar exists
    function match {
      case Function(it) => {
        val s = it.next
        val v = s.findVariableByName(name) match {
          case Some(v) => currentMin = v.asInstanceOf[Scalar] 
          case None    => return Some(Function(function.getDomain, function.getRange, Iterator.empty)) //empty Function with type of original
        }
      }
    }
    
    //Scalar "name" exists, so apply Operation to every sample from the iterator
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
    val x = sample.getVariables.map(applyToVariable(_)) 
    x.find(_.isEmpty) match { 
      case None => {          
        val s = Sample(sample.domain, sample.range)
        keepSamples += s
        Some(s)
      }
      case Some(_) => None //found an invalid variable, exclude the entire sample
    }
  }
  
  /**
   * Apply Operation to a Variable, depending on type
   */
  override def applyToVariable(variable: Variable): Option[Variable] = variable match {
    case scalar: Scalar     =>  applyToScalar(scalar)
    case sample: Sample     =>  applyToSample(sample)
    case tuple: Tuple       =>  applyToTuple(tuple)
    case function: Function =>  applyToFunction(function)
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
				  val comparison = s.compare(currentMin)
						if (comparison < 0) {
						  //Found a new min value, so update currentMin and forget old samples
						  keepSamples.clear 
						  currentMin = s
						  Some(scalar) 
					  }
						else if (comparison == 0) {
				      //Keep the sample
				      Some(scalar)
			      }
			      else {
				      //Trash this sample
				      None
			      }
			    } else { 
			        Some(scalar) //no-op
			      }
		    }
		  } catch {
		  case e: Exception => {
		    logger.warn("Min filter threw an exception: " + e.getMessage)
			  None
		  }
	  }
  }
}

object MinFilter extends OperationFactory {
  def apply(name: String): MinFilter = new MinFilter(name)
}
