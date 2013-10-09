package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import scala.collection._
import java.io.File
import scala.collection.mutable.ArrayBuffer
import latis.util.FileUtils

/**
 * Return a list of files as a Dataset.
 */
class FileListAdapter(tsml: Tsml) extends GranuleAdapter(tsml) {
  //TODO: see java 7 java.nio.file, DirectoryStream,...
  //  FileSystems.getDefault().getPathMatcher("regex:.*").matches(path)
  //Note, we can't count on the order of the files, so we can't use an iterative adapter.
  //  TODO: maybe chunk nested directories 
  
  /*
   * TODO: if file name has other parameters (e.g. instrument)
   *   then it likely has one for each time sample.
   *   Thus, 'instrument' effectively is a domain variable.
   *   Or better yet, a diff dataset
   *   best make diff tsml for now
   * (t,i)-> f
   * t->i->f
   * i->t->f
   * inst is enum list
   *   could become tuple with named elements
   *   (i1:t->f, i2:t->f,...)
   */
  
  //use url to specify root directory
  val dir: String = getProperty("url") match {
    case Some(s) => s
    case None => throw new RuntimeException("FileListAdapter requires a 'url' attribute.")
  }
  
  val regex = properties.get("pattern") match {
    case Some(s: String) => s.r
    case None => throw new RuntimeException("FileListAdapter requires a file name 'pattern' attribute.")
  }
  
  def readData: immutable.Map[String, immutable.Seq[String]] = {
    //TODO: use file or filename filter?
    //recursive
    val files = FileUtils.listAllFiles(dir)
    
    val vnames = tsml.getVariableNames
    val nvars = vnames.length
    //TODO: deal with time not being first pattern in filename
    
    //Make Map to contain the results. Make empty Seq for each variable.
    val map = mutable.HashMap[String, ArrayBuffer[String]]()
    for (vname <- vnames) map += ((vname, ArrayBuffer[String]()))
    
    //Process each filename that matches the pattern. Sort, assuming lexical order is time order.
    for (file <- files.sorted) {
      regex.findFirstMatchIn(file) match {
        case Some(m) => {
          //all but the last variable should have a match, put those in the map
          (vnames.take(nvars-1) zip m.subgroups).map(p => map += ((p._1, map(p._1) += p._2)))
          map(vnames.last) += file  //the last variable if the filename
        }
        case None => //no match, don't include this file
      }
    }
    
    //TODO: sort by domain values
    //  assume lexical ordering reflects time order, for now
    //  hard to know here what the domain var(s)
    //  should Function constructor enforce?
    
    //return as immutable dataMap
    val z = for ((name, seq) <- map) yield name -> seq.toIndexedSeq //turn ArrayBuffer into an immutable Seq
    z.toMap //immutable Map
  }
  
  override def close = {}
}