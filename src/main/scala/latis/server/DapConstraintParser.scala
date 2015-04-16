package latis.server

import scala.collection.Seq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import javax.xml.ws.http.HTTPException
import latis.ops.Operation
import latis.ops.Projection
import latis.ops.filter.Selection
import latis.util.RegEx.OPERATION
import latis.util.RegEx.SELECTION

class DapConstraintParser {
  //TODO: consider a parser combinator

  /**
   * Parse the query args into a sequence of Operations.
   */
  def parseArgs(args: Seq[String]): mutable.Seq[Operation] = {
    //buffer for accumulating the Seq of operations
    val buffer = new ArrayBuffer[Operation]() 
    
    if (args.nonEmpty) {
      //handle expressions for selections and other operations
      buffer ++= args.tail.map(parseExpression(_))
      
      //projection expression should be first among the args
      //but last to be applied, may be empty string
      if (args.head.length > 0) buffer += Projection(args(0).split(","))
 
    } //else return an empty list

    buffer.result
  }


  /**
   * Parse the individual expression into an Operation. These are based on
   * regular expression matches that will reject invalid requests.
   */
  def parseExpression(expression: String): Operation = {
    //TODO: Option? error handling
    expression match {
      case SELECTION.r(name, op, value) => Selection(name, op, value)
      case OPERATION.r(name, args) => (name,args) match {
        //for testing handling of http errors
        case ("httpError", s: String) => throw new HTTPException(s.toInt) 
        
        case (_, s: String) => Operation(name, s.split(","))
        //args will be null if there are none, e.g. first()
        case (_, null) => Operation(name)
      }
      case _ => throw new UnsupportedOperationException("Failed to parse expression: " + expression)
      //TODO: log and return None? probably should return error
    }
  }
}