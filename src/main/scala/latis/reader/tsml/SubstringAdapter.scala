package latis.reader.tsml

import latis.reader.tsml.ml.Tsml

/**
 * Use string index properties to parse ascii tabular data.
 */
class SubstringAdapter(tsml: Tsml) extends IterativeAsciiAdapter(tsml) {

  lazy val indexMap: Map[String, Array[Int]] = getProperty("substring") match {
    case Some(s: String) => {
      val vnames = getOrigScalarNames
      val specs = s.split(";")
      if (vnames.length != specs.length) throw new Error("Must have a substring specification for each variable.")
      (vnames zip specs.map(_.split(",").map(_.toInt))).toMap
    }
    case None => throw new RuntimeException("SubstringAdapter requires a substring definition.")
  }
  
  override def parseRecord(record: String): Map[String, String] = {
    
    def makeValue(name: String): Option[String] = {
      val i0 = indexMap(name)(0)
      val i1 = indexMap(name)(1)
      if (i1 > record.length) None //skip records that are too short
      else Some(record.substring(i0, i1))
    }
    
    //Map variable names to their string values.
    //Map will be empty if makeValue failed to parse thie record.
    (for (name <- getOrigScalarNames; value <- makeValue(name)) yield (name, value)).toMap
  }
  
}