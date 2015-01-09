package latis.ops

import scala.Option.option2Iterable

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
import latis.ops.agg.CollectionAggregation
import latis.ops.math.MathOperation

/**
 * Adds a new Variable to a Dataset according to the inputed math expression.
 * The str parameter must include the name of the new Variable followed by '=' and the expression.
 */
class MathExpressionDerivation(str: String) extends Operation {
  
/**
 *   If the Dataset contains a function, the derivation will be handled by applyToFunction.
 *   Otherwise, the derived variable is calculated here and added as a first level variable of the Dataset. 
 */  
  override def apply(ds: Dataset): Dataset = {
    val md = ds.getMetadata
    val vars = ds.getVariables
    val fs = vars.filter(_.isInstanceOf[Function])
    fs.length match {
      case 0 => Dataset(vars :+ Real(Metadata(str.takeWhile(_ != '=').trim), parseExpression(str.substring(str.indexOf('=')+1), ds).toSeq(0).getData), md)
      case _ => Dataset(vars.flatMap(applyToVariable(_)), md)
    }
  }
  
  /**
   * Only apply to functions.
   */
  override def applyToVariable(v: Variable): Option[Variable] = v match {
    case s: Scalar => Some(s)
    case t: Tuple => Some(t)
    case f: Function => applyToFunction(f)
  }
  
  /**
   * Adds the derived variable to the sample. 
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    val name = str.substring(0,str.indexOf('=')).trim
    val r = Real(Metadata(name), parseExpression(str.substring(str.indexOf('=')+1), sample).getVariables.head.getNumberData.doubleValue)
    Some(Sample(sample.domain, Tuple(sample.range.toSeq :+ r)))
  }
  
  /**
   * If the function doesn't contain any variables used in the derivation, return the original function.
   * Otherwise, add the derived variable to each sample.
   */
  override def applyToFunction(f: Function): Option[Variable] = {
    val s = testSample(f.getSample)
    s match {
      case None => Some(f)
      case Some(sample) => Some(Function(sample.domain, sample.range, f.iterator.map(applyToSample(_).get), f.getMetadata))
    }
  }
  
  /**
   * Tests whether a Function contains any Variables used to derive the new Variable. 
   */
  def testSample(sample: Sample): Option[Sample] = {
    val seq = str.split(Array('+','-','*','/','%','(',')'))
    for(n <- seq) 
      if(sample.findVariableByName(n)!=None) 
        return Some(Sample(sample.domain, Tuple(sample.range.toSeq :+ Real(Metadata(str.substring(0,str.indexOf('=')))))))
    None
  }
  
  /**
   * Given a string and a Dataset containing the necessary Variables, evaluates the string as a math expression. 
   */
  def parseExpression(str: String, ds: Dataset): Dataset = {
    if(str == "PI") Math.PI
    else if(str == "E") Math.E
    else try str.trim.toDouble
    catch {case e: NumberFormatException => 
      val ov = ds.findVariableByName(str.trim)
      ov match {
        case Some(v) => v
        case None => findOp(str.trim, ds)
      }
    }
  }
  
  var tempCount = 0
  
  /**
   * Evaluates one math operation in the string. 
   */
  def findOp(str: String, ds: Dataset): Dataset = { //cannot use ** for power
    if(str.contains("sqrt")) { //operations followed by (...) must be evaluated first or else the () will be lost.
      val i1 = str.indexOfSlice("sqrt")+4
      val i2 = findCloseParen(str, i1)
      val sub = str.substring(i1-4, i2+1)
      val t = MathOperation(Math.sqrt(_))(parseExpression(sub.drop(4),ds))
      tempCount += 1
      parseExpression(str.replaceAllLiterally(sub, "temp"+tempCount),
                      CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount)))
    }
    else if(str.contains("acos")) {
      val i1 = str.indexOfSlice("acos")+4
      val i2 = findCloseParen(str, i1)
      val sub = str.substring(i1-4, i2+1)
      val t = MathOperation(Math.acos(_))(parseExpression(sub.drop(4),ds))
      tempCount += 1
      parseExpression(str.replaceAllLiterally(sub, "temp"+tempCount), 
                      CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount)))
    }
    else if(str.contains("atan2")) {
      val i1 = str.indexOfSlice("atan2")+5
      val i2 = findCloseParen(str, i1)
      val sub = str.substring(i1-5, i2+1)
      val ps = str.substring(i1+1,i2).split(",")
      val t = MathOperation(Math.atan2(_,_), parseExpression(ps(1),ds))(parseExpression(ps(0),ds))
      tempCount += 1
      parseExpression(str.replaceAllLiterally(sub, "temp"+tempCount), 
                      CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount)))
    }
    else if(str.contains("sin")) {
      val i1 = str.indexOfSlice("sin")+3
      val i2 = findCloseParen(str, i1)
      val sub = str.substring(i1-3, i2+1)
      val t = MathOperation(Math.sin(_))(parseExpression(sub.drop(3),ds))
      tempCount += 1
      parseExpression(str.replaceAllLiterally(sub, "temp"+tempCount), 
                      CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount)))
    }
    else if(str.contains("cos")) {
      val i1 = str.indexOfSlice("cos")+3
      val i2 = findCloseParen(str, i1)
      val sub = str.substring(i1-3, i2+1)
      val t = MathOperation(Math.cos(_))(parseExpression(sub.drop(3),ds))
      tempCount += 1
      parseExpression(str.replaceAllLiterally(sub, "temp"+tempCount), 
                      CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount)))
    }
    else if(str.contains("deg_to_radians")) {
      val i1 = str.indexOfSlice("deg_to_radians")+14
      val i2 = findCloseParen(str, i1)
      val sub = str.substring(i1-14, i2+1)
      val t = MathOperation(Math.toRadians(_))(parseExpression(sub.drop(14),ds))
      tempCount += 1
      parseExpression(str.replaceAllLiterally(sub, "temp"+tempCount), 
                      CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount)))
    }
    
    //evaluates innermost set of (). Keeps result in appended temp Dataset so its value can be accessed later.
    else if(str.contains("(")) {
      val sub = inParen(str)
      val t = parseExpression(sub,ds)
      tempCount += 1
      parseExpression(str.replaceAllLiterally("("+sub+")", "temp"+tempCount), 
                      CollectionAggregation()(ds, t.rename(t.getName, "temp"+tempCount) ))
    }
    
    // binary operators are found in reverse order of operations because the first operator found is the last evaluated
    else if(str.contains("+") || str.contains("-")) { 
      val i = str.lastIndexOf("+") max str.lastIndexOf("-")
      if(str.charAt(i) == '+') parseExpression(str.substring(0, i),ds) + parseExpression(str.substring(i+1), ds)
      else parseExpression(str.substring(0, i), ds) - parseExpression(str.substring(i+1), ds)
    } 
    else if(str.contains("*") || str.contains("/") || str.contains("%")) {
      val i = str.lastIndexOf("*") max str.lastIndexOf("/") max str.lastIndexOf("%")
      if(str.charAt(i) == '*') parseExpression(str.substring(0, i), ds) * parseExpression(str.substring(i+1), ds)
      else if(str.charAt(i) == '/') parseExpression(str.substring(0, i), ds) / parseExpression(str.substring(i+1), ds)
      else parseExpression(str.substring(0, i), ds) % parseExpression(str.substring(i+1), ds)
    } 
    else if(str.contains("^")) {
      val i = str.lastIndexOf("^")
      parseExpression(str.substring(0,i),ds) ** parseExpression(str.substring(i+1),ds)
    }
    else throw new Exception("no operation found in string \"" + str + "\"")
    
  }
  
  /**
   * Finds the innermost set of parentheses.
   */
  def inParen(str: String): String = {
    val i1 = str.lastIndexOf("(")
    if(i1 == -1) str
    else str.slice(i1+1, str.indexOf(")",i1))
  }
  
  /**
   * Finds the close paren that matches the open paren at index. 
   */
  def findCloseParen(str: String, index: Int): Int = {
    var cp = str.indexOf(")",index)
    while(str.take(cp+1).count(_ == '(') != str.take(cp+1).count(_ == ')')) cp = str.indexOf(")", cp+1)
    cp
  }
  

}

object MathExpressionDerivation {
  def apply(str: String): MathExpressionDerivation = new MathExpressionDerivation(str)
}