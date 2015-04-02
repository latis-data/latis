package latis.reader.tsml

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import scala.Option.option2Iterable
import scala.collection.JavaConversions.asScalaIterator

import latis.reader.tsml.ml.Tsml

/**
 * Return a list of files as a Dataset.
 * Use a regular expression (defined in the tsml as 'pattern')
 * with groups to extract data values from the file names.
 */
class FileListAdapter7(tsml: Tsml) extends RegexAdapter(tsml){
  //TODO: add the file variable without defining it in the tsml? but opportunity to define max length
  //Note: Using the RegexAdapter with "()" around the file name pattern almost works.
  //      The matcher returns it first but we want the file variable to be last.
  
  /**
   * A record consists of the file name, file size.
   */
  override def getRecordIterator = {
    val dir = Paths.get(getUrl.getPath) //assumes a file URL 
    val pit = pathsIterator(dir)
    pit.map(path => dir.relativize(path).toString + "," + Files.size(path))
  }
  
  /**
   * Makes a recursive iterator of all files in the given directory and all sub directories. 
   */
  def pathsIterator(dir: Path): Iterator[Path] = {  
    asScalaIterator(Files.newDirectoryStream(dir).iterator).flatMap(path => 
      if(!Files.isDirectory(path)) Some(path)
      else pathsIterator(path))
  }
  
  /**
   * Override to add the file name (i.e. the data "record") itself as a data value.
   * Note, this assumes that the TSML has the file variable defined last.
   */
  override def extractValues(record: String) = {
    val fileName = record.split(',')(0)
    val size = record.split(',')(1)
    regex.findFirstMatchIn(fileName) match {
      case Some(m) => (m.subgroups :+ fileName) :+ size //add the file name
      case None => List[String]()
    }
  }

}