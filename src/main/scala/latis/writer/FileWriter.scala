package latis.writer

import latis.dm.Dataset
import latis.util.FileUtils

import java.io.File
import java.io.OutputStream

/**
 * Base class for Writers of formats that must be written to a file as opposed to any
 * OutputStream. If the requested OutputStream is not a FileOutputStream,
 * write to a tmp file then stream it.
 */
abstract class FileWriter extends Writer {
  //TODO: impl as trait that can be mixed in with any writer? Do OutputStreamWriter the same?
  //trait FileWriter { this: Writer =>

  def writeFile(dataset: Dataset, file: File)
  //TODO: consider requiring subclass to call getFile? less obvious?

  def write(dataset: Dataset): Unit = getOutputStream match {
    case null => {
      //No output defined so just write to file.
      //If no file name is defined, construct a default name from the dataset name.
      val file = getFile match {
        case f: File => f
        case null => {
          val f = new File(dataset.getName + "." + getProperty("suffix")) //default file name
          setFile(f) //TODO: better way? just smells wrong
          f
        }
      }
      writeFile(dataset, file)
    }
    case out: OutputStream => {
      //Write to tmp file then stream to output
      //Note, even if outputStream is a FileOutputStream, we still can't assume that subclasses can write to it.
      val tmpFile: File = FileUtils.getTmpFile
      setFile(tmpFile)
      //TODO: smelly: makes sense for subclass to implement writeFile given file
      //  more in your face that having to call getFile
      //  but feels odd since Writer has-a file, needed to support Writer(file)
      writeFile(dataset, tmpFile)
      //TODO: make sure file is closed even though contract says it should be?
      //  good idea since this is what the server will be using and wouldn't want rogue writer breaking it
      FileUtils.streamFile(tmpFile, out)
    }
  }

}