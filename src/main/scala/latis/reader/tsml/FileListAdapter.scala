package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.util.FileUtils

/**
 * Return a list of files as a Dataset.
 * Use a regular expression (defined in the tsml as 'pattern')
 * with groups to extract data values from the file names.
 */
class FileListAdapter(tsml: Tsml) extends RegexAdapter(tsml) {
  //TODO: add the file variable without defining it in the tsml? but opportunity to define max length
  //Note: Using the RegexAdapter with "()" around the file name pattern almost works.
  //      The matcher returns it first but we want the file variable to be last.
  
  /**
   * A record consists of a file name, file size.
   */
  override def getRecordIterator = {
    //TODO: can we do this without reading all file names into memory?
    //TODO: see java 7 java.nio.file, DirectoryStream,...
    //  FileSystems.getDefault().getPathMatcher("regex:.*").matches(path)
    //TODO: support ftp...?
    val dir = getUrl.getPath //assumes a file URL 
    FileUtils.listAllFilesWithSize(dir).iterator
  }
  
  /**
   * Override to add the file name (i.e. the data "record") itself as a data value.
   * Note, this assumes that the TSML has the file and file size variables defined last.
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