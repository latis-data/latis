package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.util.FileUtils
import latis.util.FileUtilsNio

/**
 * Return a list of files as a Dataset.
 * Use a regular expression (defined in the tsml as 'pattern')
 * with groups to extract data values from the file names.
 * 
 * This class is almost identical to FileListAdapter. The only
 * difference is that it uses FileUtilsNio instead of FileUtils
 * (i.e. it uses the nio package from Java7). Since we're still
 * on Java6 for the time-being, this class is experimental and
 * should be kept separate from the normal FileListAdapter.
 * At some point in the future when Java7 becomes standard
 * enough that we can use it in prod, we should merge these
 * two classes.
 * 
 * In my best tests, it appears that these methods are about
 * 10% faster than the old FileListAdapter.
 * 
 * NOTE: This adapter loads all filenames into memory. Therefore,
 * for file systems larger than about 1m files, you should use
 * StreamingFileListAdapter instead.
 */
class FileListAdapterNio(tsml: Tsml) extends RegexAdapter(tsml) {
  //TODO: add the file variable without defining it in the tsml? but opportunity to define max length
  //Note: Using the RegexAdapter with "()" around the file name pattern almost works.
  //      The matcher returns it first but we want the file variable to be last.
  
  /**
   * A record consists of a file name, file size.
   */
  override def getRecordIterator = {
    //TODO: support ftp...?
    val dir = getUrl.getPath //assumes a file URL 
    FileUtilsNio.listAllFilesWithSize(dir).iterator
  }
  
  /**
   * Override to add the file name (i.e. the data "record") itself as a data value.
   * Note, this assumes that the TSML has the file and file size variables defined last.
   */
  override def extractValues(record: String) = {
    val fileName = record.split(',')(0)
    val size = if (getOrigScalarNames.contains("fileSize")) record.split(',')(1)
      else ""
    regex.findFirstMatchIn(fileName) match {
      case Some(m) => (m.subgroups :+ fileName) :+ size //add the file name
      case None => List[String]()
    }
  }

}