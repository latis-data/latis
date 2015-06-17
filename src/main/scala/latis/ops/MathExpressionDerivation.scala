package latis.ops

import scala.Array.canBuildFrom
import com.typesafe.scalalogging.slf4j.Logging

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.implicits.doubleToDataset
import latis.dm.implicits.variableToDataset
import latis.metadata.Metadata
import latis.ops.math.BinaryMathOperation
import latis.ops.math.MathOperation
import latis.ops.math.ReductionMathOperation
import latis.ops.math.UnaryMathOperation
import scala.collection.mutable.ArrayBuffer

/**
 * Adds a new Variable to a Dataset according to the inputed math expression.
 * The str parameter must include the name of the new Variable followed by '=' and the expression.
 */
class MathExpressionDerivation(private val str: String) extends Operation with Logging {
  
  var ds: Dataset = Dataset()
  //TODO: consider defining value in an empty dataset
  
  // Each of these has to be manually implemented in applyNamedFunction
  private val supportedFunctions = Seq("DEG_TO_RAD", "ATAN2", "SQRT", "FABS", "ACOS", "ATAN", "MAG", "COS", "SIN")
  
  private val supportedConstants = Map(
      "PI" -> Math.PI,
      "E" -> Math.E
  )
  
  private lazy val varName = str.substring(0,str.indexOf('='))
  private lazy val varExpr = str.substring(str.indexOf('=')+1)
  
  private val identifierRegex = """([a-zA-Z_][a-zA-Z_0-9]*)""".r
  
  /**
   * A list of the variables that this derived field
   * depends on for its computation
   */
  lazy val dependentVars: List[String] = {
    val allIdentifiers = for (m <- identifierRegex.findAllMatchIn(varExpr)) yield m.group(1)
    allIdentifiers.
      filter(id => !supportedFunctions.contains(id)).
      filter(id => !supportedConstants.contains(id)).
      toList
  }
  
  override def apply(dataset: Dataset): Dataset = {
    val md = dataset.getMetadata
    //TODO: delegate to subclass to munge metadata
    //TODO: add provenance metadata, getProvMsg, append to "history"
    val v: Variable = dataset.unwrap match {
      case v: Variable => applyToVariable(v) match {
        case Some(v) => v
        case None => null
      }
      case null => null
    }
    
    Dataset(v, md)
  }
  
  /**
   * Only apply to functions.
   */
  override def applyToVariable(v: Variable): Option[Variable] = v match {
    case s: Scalar => Some(Tuple(Seq(s) :+ Real(Metadata(varName), parseExpression(varExpr).unwrap.getData)))
    case t: Tuple => Some(t)//Some(Tuple(t.getVariables :+ Real(Metadata(varName), parseExpression(varExpr).unwrap.getData)))
    case f: Function => applyToFunction(f)
  }
  
  /**
   * Adds the derived variable to the sample. 
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    val name = varName
    ds = sample
    val r = Real(Metadata(name), parseExpression(varExpr).unwrap.getData)
    Some(Sample(sample.domain, Tuple(sample.range.toSeq :+ r)))
  }
  
  /**
   * If the function doesn't contain any variables used in the derivation, return the original function.
   * Otherwise, add the derived variable to each sample.
   */
  override def applyToFunction(f: Function): Option[Variable] = {
    val s = testSample(f.getSample)
    s match {
      case None => {
        logger.warn("Derived field " + str.substring(0, str.indexOf('=')) + " was not added to the Dataset")
        Some(f)
      }
      case Some(sample) => Some(Function(sample.domain, sample.range, f.iterator.map(applyToSample(_).get), f.getMetadata))
    }
  }
  
  /**
   * Determines whether a Function contains all of the variables needed
   * to derive the new variable
   */
  def testSample(sample: Sample): Option[Sample] = {
    
    if (dependentVars.forall(depVar => str.contains(depVar))) {
      Some(
        Sample(
          sample.domain,
          Tuple(
            sample.range.toSeq :+ Real(Metadata(varName))
          )
        )
      )
    }
    else {
      None
    }
  }
  
  /**
   * Given a string and a Dataset containing the necessary Variables, evaluates the string as a math expression. 
   */
  def parseExpression(str: String): Dataset = {
    if (supportedConstants.contains(str)) {
      supportedConstants(str)
    }
    else {
      try str.toDouble
      catch { case e: NumberFormatException =>
        val ov = ds.findVariableByName(str)
        ov match {
          case Some(v) => v
          case None => findOp(str)
        }
      }
    }
  }
  
  var tempCount = 0
  
  /**
   * Finds and evaluates one operation in the expression.
   */
  def findOp(str: String): Dataset = {
    //named operations followed by (...) must be evaluated first or else the () will be lost.
    //names should be looked for in order from longest to shortest to prevent errors with substrings such as "cos" in "acos".
    val names = supportedFunctions.filter(str.contains(_))
    if(names.nonEmpty) applyNamedFunction(str, names(0))
 
    //evaluates innermost set of (). Keeps result in appended temp Dataset so its value can be accessed later.
    else if(str.contains("(")) {
      val sub = inParen(str)
      val t: Variable = parseExpression(sub).unwrap
      tempCount += 1
      ds = ds.unwrap match {
        case null => Dataset(t.rename(t.getName, "temp"+tempCount).unwrap)//CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount))
        case _ => Dataset(Tuple(ds.unwrap, t.rename(t.getName, "temp"+tempCount).unwrap))//CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount))
      }
      parseExpression(str.replaceAllLiterally("("+sub+")", "temp"+tempCount))
    }
    
    // basic math operators are found in reverse order of operations because the first operator found is the last evaluated
    // except for ^ (exponentiation) which is the other way around because it's right-associative
    else if(str.contains("&")) applyBasicMath(str, str.lastIndexOf("&"))
    else if(str.contains("<")) applyBasicMath(str, str.lastIndexOf("<"))
    else if(str.contains("+") || str.contains("-")) applyBasicMath(str, str.lastIndexOf("+") max str.lastIndexOf("-"))
    else if(str.contains("*") || str.contains("/") || str.contains("%")) applyBasicMath(str, str.lastIndexOf("*") max str.lastIndexOf("/") max str.lastIndexOf("%"))
    else if(str.contains("^")) applyBasicMath(str, str.indexOf("^"))

    else throw new Exception("no operation found in expression \"" + str + "\"")
    
  }
  
  def applyNamedFunction(str: String, name: String) = {
    val i1 = str.indexOf(name)
    val i2 = findCloseParen(str, i1) + 1
    val sub = str.substring(i1, i2)
    val args = str.substring(i1+name.length+1,i2-1)
    val op = name match {
      case "ATAN2" => MathOperation(Math.atan2(_,_))
      case "MAG" => MathOperation((d1,d2) => Math.sqrt(Math.pow(d1,2) + Math.pow(d2,2)))
      case "COS" => MathOperation(Math.cos(_))
      case "SIN" => MathOperation(Math.sin(_))
      case "FABS" => MathOperation(Math.abs(_))
      case "ACOS" => MathOperation(Math.acos(_))
      case "SQRT" => MathOperation(Math.sqrt(_))
      case "DEG_TO_RAD" => MathOperation(Math.toRadians(_))
      case "ATAN" => MathOperation(Math.atan(_))
    }
    val t: Variable = op match {
      case u: UnaryMathOperation => u(parseExpression(args)).unwrap
      case b: BinaryMathOperation => ???
      case r: ReductionMathOperation => r(split(args).map(parseExpression(_))).unwrap
    }

    tempCount += 1
    ds = ds.unwrap match {
      case null => Dataset(t.rename(t.getName, "temp"+tempCount).unwrap)//CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount))
      case _ => Dataset(Tuple(ds.unwrap, t.rename(t.getName, "temp"+tempCount).unwrap))//CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount))
    }
    parseExpression(str.replaceAllLiterally(sub, "temp"+tempCount))
  }
   
  /**
   * Split only on commas that are not within parentheses. 
   */
  def split(args: String): Seq[String] = {
    val buffer = ArrayBuffer[String]()
    var c1 = -1
    var c2 = args.indexOf(',')
    while(c2 != -1){
      val sub = args.substring(c1+1, c2)
      if(sub.count(_=='(')>sub.count(_==')')) c2 = args.indexOf(',',c2+1)
      else {
        buffer += sub
        c1 = c2
        c2 = args.indexOf(',',c2+1)
      }
    }
    buffer += args.substring(c1+1)
    buffer.toSeq
  }
  
  def applyBasicMath(str: String, i: Int) = {
    val op = str.substring(i, i+1)
    val lhs = parseExpression(str.substring(0,i))
    val rhs = parseExpression(str.substring(i+1))
    op match {
      case "+" => lhs + rhs
      case "-" => lhs - rhs
      case "*" => lhs * rhs
      case "/" => lhs / rhs
      case "%" => lhs % rhs
      case "^" => lhs ** rhs
      case "<" => lhs < rhs
      case "&" => lhs && rhs
      case _ => throw new Exception("unknown operation: " + op)
    }
  } 
  
  /**
   * Return the contents of the innermost set of parentheses.
   */
  def inParen(str: String): String = {
    val i1 = str.lastIndexOf("(")
    if(i1 == -1) str
    else str.slice(i1+1, str.indexOf(")",i1))
  }
  
  /**
   * Finds the close paren that matches the next open paren after index. 
   */
  def findCloseParen(str: String, index: Int): Int = {
    var cp = str.indexOf(")",index)
    while(str.take(cp+1).drop(index).count(_ == '(') != str.take(cp+1).drop(index).count(_ == ')')) 
      cp = str.indexOf(")", cp+1)
    cp
  }

}

object MathExpressionDerivation {
  def apply(str: String): MathExpressionDerivation = new MathExpressionDerivation(str.filter(_ != ' '))
}