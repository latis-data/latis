package latis.data

trait TextData extends Data {
  /*
   * TODO: should we treat Text/Strings as a char array?
   * require length?
   * default to 4 chars = 8 bytes
   */
  
  def toString: String
  //def iterator: Iterator[String]
}
