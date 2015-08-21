package latis.util

import latis.data.Data
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Text
import latis.dm.Variable

/**
 * Utility methods for manipulating Strings.
 */
object StringUtils {
    
  /**
   * Resolve any property references in the given string.
   * e.g. ${dataset.dir}
   * This will ask LatisProperties which has access to properties from
   * the latis.properties file, systems properties, and environment variables.
   */
  def resolveParameterizedString(s: String): String = {
    //val pattern = """${(.+)}""".r //TODO: restrict to \w\\.
    //pattern.replaceAllIn(s, (m: Match) => LatisProperties(m.matched))
    //TODO: support more than one parameter?
    //TODO: default with ${foo:-default} like logback (and bash)
    
    s.indexOf("${") match {
      case -1 => s
      case i1: Int => s.indexOf('}', i1) match {
        case -1 => s
        case i2: Int => {
          val prop = s.substring(i1 + 2, i2)
          LatisProperties.get(prop) match {
            case Some(p) => s.substring(0,i1) + p + s.substring(i2 + 1)
            case None => s //TODO: error, property not found? but would disallow legit values of that form
          }
        }
      }
    }
  }
  
  /**
   * Replaces any non-word characters (not a letter or number) with that character escaped.
   */
  def escapeNonWords(str: String) = """\W""".r.replaceAllIn(str, "\\\\" + _)
  
  /**
   * Return the given string padded or truncated to the given length (number of characters).
   * If the String is shorter than the desired length, it will be padded with spaces on the right.
   * If the String is longer than the desired length, it will be truncated on the right.
   */
  def padOrTruncate(s: String, length: Int): String = s.length match {
    case l: Int if (l < length) => s.padTo(length, ' ')
    case l: Int if (l > length) => s.substring(0, length)
    //otherwise, the size is just right
    case _ => s
  }
  /**
   * Return the given string padded or truncated to the length of the given Variable.
   * If the Variable is not Text, return the original string.
   */
  def padOrTruncate(s: String, template: Variable): String = template match {
    case t: Text => padOrTruncate(s, t.length)
    case _ => s
  }
  
  /**
   * Can this string be converted to a Double.
   */
  def isNumeric(s: String): Boolean = {
    try {
      s.toDouble
      true
    } catch {
      case e: NumberFormatException => false
    }
  }
  
  /**
   * Convert the given String to a Double.
   * If not convertible, return NaN.
   */
  def toDouble(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case e: Exception => Double.NaN
    }
  }
  
  /**
   * construct Data from a String by matching a Variable template
   */
  //TODO: support regex property for each variable
  def parseStringValue(value: String, variableTemplate: Variable): Data = variableTemplate match {
    case _: Integer => try {
      //If value looks like a float, take everything up to the decimal point.
      val s = if (value.contains(".")) value.substring(0, value.indexOf("."))
      else value
      Data(s.trim.toLong)
    } catch {
      case e: NumberFormatException => Data(variableTemplate.asInstanceOf[Integer].getFillValue.asInstanceOf[Long])
    }
    case _: Real => try {
      Data(value.trim.toDouble)
    } catch {
      case e: NumberFormatException => Data(variableTemplate.asInstanceOf[Real].getFillValue.asInstanceOf[Double])
    }
    case t: Text    => Data(StringUtils.padOrTruncate(value, t.length)) //enforce length
  }
}