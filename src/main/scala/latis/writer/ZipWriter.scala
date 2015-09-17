package latis.writer

import java.io.BufferedReader
import java.io.File
import java.io.FileOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import scala.io.Source

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Text

/**
 * Write a zip file of the files contained in a file list dataset.
 */
class ZipWriter extends FileWriter {
  
  def writeFile(dataset: Dataset, file: File) = {
    
    lazy val dir = dataset.getMetadata.get("srcDir") match {
      case Some(sd) => sd
      case None => "" //???
    }
    
    //Get the name of each file as it appears in the file list.
    //Throw an exception if a 'file' Variable cannot be found in the Dataset.
    val files = dataset.findVariableByName("file") match {
      case Some(v) => dataset match {  
        case Dataset(Function(it)) => it.map(_.findVariableByName("file") match {
          case Some(Text(t)) => t
        })
      }
      case None => throw new IllegalArgumentException("ZipWriter can only write Datasets that contain a 'file' Variable.")
    }
    
    /**
     * A stream of values from the given BufferedReader.
     * A value of -1 will be returned when there are no more values
     * to be read, but the stream will never be empty.
     */
    def readerStream(bufferedReader: BufferedReader) = {
      //when there is nothing left to read, a value of -1 will be returned, but the stream will never be empty.
      Stream.continually(bufferedReader.read)
    }
    
    val zip = new ZipOutputStream(new FileOutputStream(file))
    try {
      for (f <- files) {
        //add zip entry to output stream
        zip.putNextEntry(new ZipEntry(f))

        val in = Source.fromFile(dir + File.separator + f).bufferedReader
        try {
          readerStream(in).takeWhile(_ > -1).foreach(zip.write(_))
        }
        finally {
          in.close
        }

        zip.closeEntry
      }
    }
    finally {
      zip.close
    }
  }
  
  override def mimeType = "application/zip"
  
}