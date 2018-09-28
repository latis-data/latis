package latis.ops

import scala.annotation.migration
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.combinator.PackratParsers
import latis.dm._
import latis.metadata.Metadata
import latis.ops.math.BinOp.ADD
import latis.ops.math.BinOp.AND
import latis.ops.math.BinOp.DIVIDE
import latis.ops.math.BinOp.LT
import latis.ops.math.BinOp.MODULO
import latis.ops.math.BinOp.MULTIPLY
import latis.ops.math.BinOp.POWER
import latis.ops.math.BinOp.SUBTRACT
import latis.time.Time

/**
 * Adds a new Variable to a Dataset according to the inputed math expression.
 * The str parameter must include the name of the new Variable followed by '=' and the expression.
 */
class MathExpressionDerivation(private val str: String) extends Operation {
  
  /**
   * The name of the Variable being derived.
   */
  private lazy val varName = str.take(str.indexOf('='))
  /**
   * The expression used to derive the new Variable.
   */
  private lazy val varExpr = str.drop(str.indexOf('=')+1)
  
  /**
   * The parsed expression. This is an abstract syntax tree representing 
   * the derivation expression which can be evaluated using:
   *   eval(parsedExpr, values: Map[String, Double])
   * where 'values' maps any named variables in the derivation to their values.
   */
  private lazy val parsedExpr = MathParser(varExpr).getOrElse(throw new
      Exception(s"Unable to parse expression: $varExpr"))
  
  def eval(expr: Expr, values: Map[String, Double]): Double = expr match {
    case NumExpr(n) => n
    case IdExpr(id) => values(id)
    case OpExpr(op, dl @ _*) => foldOps.get(op) match {
      case Some(f) => f(dl.map(eval(_, values)): _*)
      case None => binOps.get(op) match {
        case Some(f) if(dl.length == 2)=> f(eval(dl(0), values), eval(dl(1), values))
        case _ => unOps.get(op) match {
          case Some(f) if(dl.length == 1)=> f(eval(dl(0), values))
          //If we get here, we probably have the wrong number of parameters for the operation
          case _ => throw new RuntimeException(s"Could not evaluate $op with parameters: ${dl.map(eval(_,values))}")
        }
      }
    }
  }    
      
  /**
   * Evaluate the derivation expression with whatever values
   * are currently in 'values'.
   */
  private def deriveField = eval(parsedExpr, values)
      
  /**
   * Test whether the given dataset contains all of the variables
   * needed to evaluate the parsed expression, and all operations
   * are known.
   */
  override def apply(ds: Dataset): Dataset = {
    def canEval(exp: Expr, names: Seq[String]): Boolean = exp match {
      case NumExpr(n) => true
      case IdExpr(id) => names.contains(id)
      case OpExpr(op, dl @ _*) => {
        dl.forall(canEval(_, names)) &&
        (foldOps.keySet ++ binOps.keySet ++ unOps.keySet).contains(op)
      }
    }
    val names = ds match {
      case Dataset(v) => v.toSeq.map(_.getName) ++ values.keySet
      case _ => values.keySet.toSeq //constants
    }
    if(canEval(parsedExpr, names)) super.apply(ds)
    else ds //can't apply derivation, so return original dataset
  }
   
  
  /**
   * Keep a map of each Number Scalar to its value.
   */
  private var values: Map[String, Double] = Map("E" -> (Math.E), "PI" -> (Math.PI))

  /**
   * If this is the scalar being derived, update its Data.
   * Index will be transformed to Real if it is being derived. 
   */
  override def applyToScalar(s: Scalar): Option[Scalar] = {
    if (s.hasName(varName)) s match {
      case t: Time => 
        val units = t.getMetadata("units") match {
          case Some(u) if (!t.isInstanceOf[Text]) => u //preserve numeric units
          case _ => "milliseconds since 1970"
        }
        Some(Time(t.getMetadata + ("units" -> units), deriveField))
      case i: Index => Some(Real(i.getMetadata, deriveField)) //replace Index with Real
      case i: Integer => Some(Integer(i.getMetadata, deriveField))
      case r: Real => Some(Real(r.getMetadata, deriveField))
      case Text(s) => throw new RuntimeException("MathExpressionDerivation cannot be applied to Text Variables.")
    } else Some(s)
  }
  
  /**
   * First add all of this Sample's data to 'values'.
   * If a brand new Variable is being derived, add it to the range
   * (with type Real). Otherwise delegate to applyToVariable to replace
   * the value that already exists for the derived field.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    sample.toSeq.foreach(s => s match {
      case Index(i) => values = values + (s.getName -> i)
      case Integer(i) => values = values + (s.getName -> i)
      case Number(d) => values = values + (s.getName -> d)  //Real or Time as Text
    })
    sample.range.findFunction match {
      case None if (!values.keySet.contains(varName)) => {
        val r = Real(Metadata(varName), deriveField)
        Some(Sample(sample.domain, Tuple(sample.range.toSeq :+ r)))
      } 
      case _ => {
        val d = applyToVariable(sample.domain)
        val r = applyToVariable(sample.range)
        (d,r) match {
          case (Some(d), Some(r)) => Some(Sample(d, r))
          case _ => ??? //applyToVariable should always return Some
        }
      }
    }
  }
  
  
  
//---------------- Parsing ---------------------------------------------
  
  //Map of supported unary operations
  lazy val unOps: Map[String, Double => Double] = Map(
    "-"          -> (_ * -1),
    "SIN"        -> (Math.sin(_)),
    "COS"        -> (Math.cos(_)),
    "ACOS"       -> (Math.acos(_)),
    "FABS"       -> (Math.abs(_)),
    "SQRT"       -> (Math.sqrt(_)),
    "ATAN"       -> (Math.atan(_)),
    "DEG_TO_RAD" -> (Math.toRadians(_))
  )
  //Map of supported binary operations
  lazy val binOps: Map[String, (Double, Double) => Double] = Map(
    "+"     -> ADD,
    "-"     -> SUBTRACT,
    "*"     -> MULTIPLY,
    "/"     -> DIVIDE,
    "%"     -> MODULO,
    "^"     -> POWER,
    "<"     -> LT,
    "&"     -> AND,
    "ATAN2" -> (Math.atan2(_,_))
  ).withDefault(x => foldOps(x)(_,_))
  //Map of operations that take indefinitely many inputs
  lazy val foldOps: Map[String, (Double*) => Double] = Map(
    "MAG" -> (dl => Math.sqrt(dl.foldLeft(0.0)(_ + Math.pow(_,2))))
  )
  
  //classes for an abstract syntax tree representing the parsed expression
  trait Expr
  case class NumExpr(n: Double) extends Expr
  case class IdExpr(id: String) extends Expr
  case class OpExpr(op: String, exprs: Expr*) extends Expr 
  
  object MathParser extends JavaTokenParsers with PackratParsers {
    
    lazy val expr = and
    
    //parse boolean and
    lazy val and: PackratParser[Expr] = 
      (lt ~ "&" ~ and ^^ {case lt ~ op ~ and => OpExpr(op, lt, and)}
        | lt )
    
    //parse less than
    lazy val lt: PackratParser[Expr] = 
      (pm ~ "<" ~ lt ^^ {case pm ~ op ~ lt => OpExpr(op, pm, lt)}
        | pm )
    
    //parse top level addition and subtraction
    lazy val pm: PackratParser[Expr] = 
      (pm ~ ("+"|"-") ~ md ^^ {case pm ~ op ~ md => OpExpr(op, pm, md)}
        | md )
        
    //parse top level multiplication and division
    lazy val md: PackratParser[Expr] = 
      (md ~ ("*"|"/"|"%") ~ pow ^^ {case md ~ op ~ p => OpExpr(op, md, p)}
        | pow )
    
    //parse top level exponentiation
    lazy val pow: PackratParser[Expr] = 
      (value ~ "^" ~ pow ^^ {case v ~ op ~ p => OpExpr(op, v, p)}
        | value )
        
    //parse a named function, expression in parentheses, variable name, or number
    lazy val value: PackratParser[Expr] = 
      ( "-" ~ value                                    ^^ {case op ~ v => OpExpr(op, v)}
        | ident ~ ("(" ~> rep(",".? ~> expr) <~ ")")   ^^ {case id ~ es => OpExpr(id, es:_*)}
        | ("(" ~> expr <~ ")")                         ^^ {case e => e}
        | ident                                        ^^ {case id => IdExpr(id)}
        | floatingPointNumber                          ^^ {case n => NumExpr(n.toDouble)}
      )
      
    def apply(str: String): MathParser.ParseResult[Expr] = parseAll(expr, str)
  }

}

object MathExpressionDerivation extends OperationFactory {
  def apply(str: String): MathExpressionDerivation = new MathExpressionDerivation(str.filter(_ != ' '))
  
  override def apply(args: Seq[String]): MathExpressionDerivation = MathExpressionDerivation(args.mkString(","))
}
