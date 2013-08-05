package latis.data

trait TextData extends Any with Data {
  /*
   * TODO: should we treat Text/Strings as a char array?
   * require length?
   * default to 4 chars = 8 bytes
   */
  
  def stringValue: String
}

//object TextData {
//just encourages instantiation of value class
//  def unapply(text: TextData) = Some(text.stringValue)
//}