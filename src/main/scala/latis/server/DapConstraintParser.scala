package latis.server

import latis.ops._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import latis.util.RegEx._
import java.net.URLDecoder

object DapConstraintParser {

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
  
//  /**
//   * Parse the query string into a sequence of Operations.
//   */
//  def parseQuery(query: String): Seq[Operation] = {
//    
//    //buffer for accumulating the Seq of operations
//    val buffer = new ArrayBuffer[Operation]() 
//    
//    if (query != null) {
//      //decode special characters (e.g. >,<)
//      val q = URLDecoder.decode(query, "UTF-8") //TODO: TSDS uses ISO-8859-1
//      
//      //break up query into individual expressions
//      val ss = q.split("&")
//
//      //projection expression should be first, may be empty
//      if (ss.head.length > 0) buffer += Projection(ss(0).split(","))
//
//      //handle expressions for selections and other operations
//      //for (s <- ss.tail) buffer += parseExpression(s)
//      buffer ++= ss.tail.map(parseExpression(_))
// 
//  //try projection last    
//      //made Function instead of wrapping
//  //if (ss.head.length > 0) buffer += ProjectionFilter(ss(0).split(","))
// 
//    } //else return an empty list
//
//    buffer.result
//  }

  /**
   * Parse the individual expression into an Operation.
   */
  def parseExpression(expression: String): Operation = {
    //TODO: Option? error handling
    expression match {
      case SELECTION.r(name, op, value) => Selection(name, op, value)
      case OPERATION.r(name, args) => Operation(name, args.split(","))
      case _ => throw new UnsupportedOperationException("Failed to parse expression: " + expression)
      //TODO: log and return None? probably should return error
    }
  }
}