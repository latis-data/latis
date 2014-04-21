package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.data.Data

/**
 * Use string index properties to parse ascii tabular data.
 */
class SubstringAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {

//  lazy val indexMap: Map[String, Array[Int]] = getProperty("substring") match {
//    case Some(s: String) => {
//      val vnames = getOrigScalarNames
//      val specs = s.split(";")
//      if (vnames.length != specs.length) throw new Error("Must have a substring specification for each variable.")
//      (vnames zip specs.map(_.split(",").map(_.toInt))).toMap
//    }
//    case None => throw new RuntimeException("SubstringAdapter requires a substring definition.")
//  }
  
  lazy val substringIndices: Seq[Array[Int]] = getProperty("substring") match {
    case Some(s: String) => s.split(";").map(p => p.split(",").map(_.toInt))
    case None => throw new RuntimeException("SubstringAdapter requires a substring definition.")
  }
  
  override def extractValues(record: String): Seq[String] = {
    substringIndices.flatMap( is => {
      val i0 = is(0)
      val i1 = is(1)
      if (i1 > record.length) None 
      //Note, if record is too short, the size of the returned Seq will cause the superclass parseRecord to drop this record.
      //TODO: replace with fill value
      else Some(record.substring(i0, i1))
    })
  }
  
//  override def parseRecord(record: String): Option[Map[String, Data]] = {
//    
//    def makeValue(name: String): Option[String] = {
//      val i0 = indexMap(name)(0)
//      val i1 = indexMap(name)(1)
//      if (i1 > record.length) None //skip records that are too short
//      else Some(record.substring(i0, i1))
//    }
//    
//    //Map variable names to their string values.
//    //Map will be empty if makeValue failed to parse thie record.
//    (for (name <- getOrigScalarNames; value <- makeValue(name)) yield (name, value)).toMap
//  }
  
}