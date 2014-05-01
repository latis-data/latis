package latis.util

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
}