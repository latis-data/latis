package latis.ops

import scala.annotation.migration
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.combinator.PackratParsers

import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
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
      
  /**
   * Evaluate the derivation expression with whatever values
   * are currently in 'values'.
   */
  private def deriveField = eval(parsedExpr, values)
      
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
      case i: Index => Some(Real(i.getMetadata, deriveField)) //replace Index with Real
      case i: Integer => Some(Integer(i.getMetadata, deriveField))
      case r: Real => Some(Real(r.getMetadata, deriveField))
      case Text(s) => throw new Exception("MathExpressionDerivation cannot be applied to Text Variables.")
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
      case Real(d) => values = values + (s.getName -> d)
      case Integer(i) => values = values + (s.getName -> i)
      case t: Time => values = values + (t.getName -> t.getJavaTime)
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
    "MAG"   -> (foldOps("MAG")(_,_)),
    "ATAN2" -> (Math.atan2(_,_))
  )
  //Map of operations that take indefinitely many inputs
  lazy val foldOps: Map[String, (Double*) => Double] = Map(
    "MAG" -> (dl => Math.sqrt(dl.foldLeft(0.0)(_ + Math.pow(_,2))))
  )
  
  //classes for an abstract syntax tree representing the parsed expression
  trait Expr
  case class NumExpr(n: Double) extends Expr
  case class IdExpr(id: String) extends Expr
  case class UnOpExpr(op: String, expr: Expr) extends Expr 
  case class BinOpExpr(op: String, left: Expr, right: Expr) extends Expr
  case class FoldOpExpr(op: String, exprs: Expr*) extends Expr 
  def eval(expr: Expr, values: Map[String, Double]): Double = expr match {
    case NumExpr(n) => n
    case IdExpr(id) => values(id)
    case UnOpExpr(op, e) => unOps(op)(eval(e, values))
    case BinOpExpr(op, l, r) => binOps(op)(eval(l, values), eval(r, values))
    case FoldOpExpr(op, dl @ _*) => foldOps(op)(dl.map(eval(_, values)): _*)
  }
  
  object MathParser extends JavaTokenParsers with PackratParsers {
    
    lazy val expr = and
    
    //parse boolean and
    lazy val and: PackratParser[Expr] = 
      (lt ~ "&" ~ and ^^ {case lt ~ op ~ and => BinOpExpr(op, lt, and)}
        | lt )
    
    //parse less than
    lazy val lt: PackratParser[Expr] = 
      (pm ~ "<" ~ lt ^^ {case pm ~ op ~ lt => BinOpExpr(op, pm, lt)}
        | pm )
    
    //parse top level addition and subtraction
    lazy val pm: PackratParser[Expr] = 
      (pm ~ ("+"|"-") ~ md ^^ {case pm ~ op ~ md => BinOpExpr(op, pm, md)}
        | md )
        
    //parse top level multiplication and division
    lazy val md: PackratParser[Expr] = 
      (md ~ ("*"|"/"|"%") ~ pow ^^ {case md ~ op ~ p => BinOpExpr(op, md, p)}
        | pow )
    
    //parse top level exponentiation
    lazy val pow: PackratParser[Expr] = 
      (value ~ "^" ~ pow ^^ {case v ~ op ~ p => BinOpExpr(op, v, p)}
        | value )
        
    //parse a named function, expression in parentheses, variable name, or number
    lazy val value: PackratParser[Expr] = 
      ( "-" ~ value                                    ^^ {case op ~ v => UnOpExpr(op, v)}
        | ident ~ ("(" ~> expr <~ ")")                 ^^ {case id ~ e => UnOpExpr(id, e)}
        | ident ~ ("(" ~> expr) ~ ("," ~> expr <~ ")") ^^ {case id ~ l ~ r => BinOpExpr(id, l, r)}
        | ident ~ ("(" ~> rep(",".? ~> expr) <~ ")")   ^^ {case id ~ es => FoldOpExpr(id, es:_*)}
        | ("(" ~> expr <~ ")")                         ^^ {case e => e}
        | ident                                        ^^ {case id => IdExpr(id)}
        | floatingPointNumber                          ^^ {case n => NumExpr(n.toDouble)}
      )
      
    def apply(str: String) = parseAll(expr, str)
  }

}

object MathExpressionDerivation {
  def apply(str: String): MathExpressionDerivation = new MathExpressionDerivation(str.filter(_ != ' '))
}