package latis.reader.tsml

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.Option.option2Iterable
import scala.collection.JavaConversions.asScalaIterator
import latis.reader.tsml.ml.Tsml
import java.nio.file.DirectoryStream
import java.io.Closeable
import scala.collection.mutable.Stack

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
   * 
   * The caller is responsible for calling close() on the resulting iterator if iteration
   * is not run to completion.
   */
  def pathsIterator(dir: Path): FlatPathIterator = new FlatPathIterator(dir) 
  
  /**
   * Override to add the file name (i.e. the data "record") itself as a data value.
   * Note, this assumes that the TSML has the file variable defined last.
   */
  override def extractValues(record: String) = {
    val chunks = record.split(',')
    if (chunks.length != 2) throw new Exception("\"" + record + "\" does not fit expected record pattern \"file name, file size\"")
    val fileName = chunks(0)
	val size = chunks(1)
    regex.findFirstMatchIn(fileName) match {
      case Some(m) => (m.subgroups :+ fileName) :+ size //add the file name and size
      case None => List[String]()
    }
  }
  
  /**
   * Utility class that will recursively iterate over a folder and return
   * all of the files contained in it, or in any child folders. It is
   * not possible to tell from the returned values whether they came from
   * the parent directory or a child directory - this represents a flatMap
   * operation.
   * 
   * Internally this uses DirectoryStream objects for iteration, which
   * must be closed. Recursive DirectoryStream objects are closed automatically
   * when their hasNext() method returns false. If the FlatPathIterator
   * is iterated to completion (until its hasNext() method returns false)
   * then all DirectoryStreams will be closed automatically. If iteration
   * is terminated early ("ok, I found the file I'm looking for, let's stop now!")
   * then the caller is responsible for calling the close() method which
   * will close any open DirectoryStream objects.
   */
  class FlatPathIterator(dir: Path) extends Iterator[Path] with Closeable {
    
    /**
     * Private data structure to store DirectoryStreams and their iterators.
     * The 'top' of the stack is the deepest folder that we currently have
     * open, and also the one which we're currently iterating over.
     * 
     * (We're performing a depth-first traversal of the folders)
     */
    private val dirStack: Stack[Pair[DirectoryStream[Path], Iterator[Path]]] = new Stack()
    pushAndInit(dir)
    
    /**
     * Use dirStack to get the next available Path, regardless of whether
     * it represents a Directory or a File.
     * 
     * This method is probably only useful to the getNextFile function
     */
    private def getNextPath(): Option[Path] = {
      do {
        val top = dirStack.top
        val iter = top._2
        if (iter.hasNext) {
          return Some(iter.next)
        }
        else {
          popAndClose()
        }
      } while (!dirStack.isEmpty)
      return None
    }
    
    /**
     * Use dirStack to get the next available Path that is also a File.
     * If Directories are encountered they will be traversed in a
     * depth-first order until the next File is found.
     * 
     * This method will also set 'next' variable to its return value,
     * for future use.
     */
    private def getNextFile(): Option[Path] = {
      while (true) {
        // In general: keep calling getNextPath until we get a File.
        // * If we get back a path, return it
        // * If we get back a directory, push it on the stack and try again (search that directory)
        // * If we get back None, that means we've finished searching all directories
        //    so we should also give up and return None.
        getNextPath() match {
          case None => {
            return None
          }
          case Some(nextPath: Path) if !Files.isDirectory(nextPath) => {
            return Some(nextPath)
          }
          case Some(nextPath: Path) => {
            pushAndInit(nextPath) // continue to next iteration without returning (i.e. call getNextAnythign again)
          }
          case _ => throw new Exception("This should be unreachable code")
        }
      }
      
      // This makes the compiler happy (otherwise it thinks we're trying to return the
      // Unit return value of the while loop)
      throw new Exception("How on earth did you get here? This should be unreachable code.")
    }
    
    /**
     * This method should be used in lieu of manually pushing to dirStack.
     * 
     * Given a Path, get a DirectoryStream for it and push that DirectoryStream
     * and its iterator onto dirStack
     */
    private def pushAndInit(path: Path) = {
      val dirStream = Files.newDirectoryStream(dir)
      val iter = dirStream.iterator()
      dirStack.push((dirStream, iter))
    }
    
    /**
     * This method should be used in lieu of manually popping from dirStack.
     * 
     * Pop the top DirectoryStream/Iterator pair from dirStack and call
     * close() on the DirectoryStream. Return the popped pair.
     */
    private def popAndClose(): Pair[DirectoryStream[Path], Iterator[Path]] = {
      val pair = dirStack.pop()
      val dirStream = pair._1
      dirStream.close()
      pair
    }
    
    /**
     * Private cache variable used by hasNext/next. This is necessary
     * because we're performing a filter operation as well as a 
     * flatMap operation (we must filter out Paths that are also
     * Directories). In order to know if we have any elements left
     * to iterate over (hasNext), we must successfully retrieve a satisfactory
     * element from the underlying iterator. Since the underlying
     * iterator cannot return that value twice, we must cache
     * that value here for future use (next).
     * 
     * nextFile has 3 allowable values:
     * 1. Some(Path) - We have searched for a File and found one
     * 2. None - We have searched for a File and run out of places
     *        to search. Iteration should be considered complete.
     * 3. null - We have not searched for a File yet. You cannot
     *        rely on the value of this variable at this time.
     */
    private var nextFile: Option[Path] = null
    
    /**
     * Returns true if there are more items to iterate over.
     */
    def hasNext(): Boolean = {
      // Internal Postcondition: nextFile will not be null after a call
      // to hasNext.
      if (nextFile == null) {
        nextFile = getNextFile()
      }
      return nextFile.isDefined
    }
    
    /**
     * Return the next available element, or throw a
     * NoSuchElementException if none are available.
     */
    def next(): Path = {
      
      // 99.99% of the time this should be unnecessary, because
      // the user will call hasNext which will always populate
      // the next variable. However, we can't rely on that, so
      // we must set it ourselves if it has not been set (for
      // example, the user could wrap this in a try block
      // and just iterate until an Exception is thrown - it's
      // an ugly but allowed way to iterate over an Iterator)
      if (nextFile == null) {
        nextFile = getNextFile()
      }
      
      if (!nextFile.isDefined) {
        throw new NoSuchElementException()
      }
      val result = nextFile.get
      nextFile = null
      return result
    }
    
    def close() = {
      while (!dirStack.isEmpty) {
        popAndClose()
      }
    }
  }
}