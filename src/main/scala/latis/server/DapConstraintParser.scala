package latis.server

import scala.collection.Seq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import javax.xml.ws.http.HTTPException
import latis.ops.Operation
import latis.ops.Projection
import latis.ops.NoOp
import latis.ops.filter.Selection
import latis.util.RegEx.PROJECTION
import latis.util.RegEx.OPERATION
import latis.util.RegEx.SELECTION

class DapConstraintParser {
  //Note, using a class instead of object to avoid concurrency issues.
  //TODO: consider a parser combinator

  /**
   * Parse the query args into a sequence of Operations.
   */
  def parseArgs(args: Seq[String]): mutable.Seq[Operation] = {
    //buffer for accumulating the Seq of operations
    val buffer = new ArrayBuffer[Operation]() 
    
    if (args.nonEmpty) {
      //parse expressions for projections, selections, and filters
      buffer ++= args.filter(_.nonEmpty).map(parseExpression(_))
 
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
      case PROJECTION.r(name) => Projection(name)
      case SELECTION.r(name, op, value) => Selection(name, op, value)
      case OPERATION.r(name, args) => (name,args) match {
        //for testing handling of http errors
        case ("httpError", s: String) => throw new HTTPException(s.toInt) 
        
        case (_, s: String) => Operation(name, s.split(","))
        //args will be null if there are none, e.g. first()
        case (_, null) => Operation(name)
      }
      case _ => throw new UnsupportedOperationException("Failed to parse expression: '" + expression + "'")
      //TODO: log and return None? probably should return error
    }
  }
}