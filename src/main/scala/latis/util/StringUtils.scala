package latis.util

object StringUtils {
  
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