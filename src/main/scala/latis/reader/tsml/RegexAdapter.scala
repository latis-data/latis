package latis.reader.tsml

class RegexAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {
  
  
  
  /**
   * Parse a "record" of text
   * into a Map of Variable name to value. This may be one or more lines
   * as defined by the "linesPerRecord" attribute of this adapter definition 
   * in the TSML.
   * Note, LinkedHashMap will maintain order.
   * Return empty Map if there was a problem with this record.
   */
  override def parseRecord(record: Record): Map[Name, Value] = {
    //assume one line per record, space delimited
    (variableNames zip record(0).split(" ")).toMap
  }
}