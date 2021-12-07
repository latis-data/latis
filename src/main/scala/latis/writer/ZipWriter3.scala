package latis.writer

import java.io._
import java.net._
import java.util.zip._

import com.typesafe.scalalogging.LazyLogging
import latis.dm._
import latis.ops.BinaryListToZipList
import latis.ops.FileListToZipList
import latis.ops.UrlListToZipList
import latis.time.Time

/**
 * Proposed replacement for ZipWriter1 and ZipWriter2 (LATIS-476).
 * Use operations to transform binary, "file", or "url" list datasets into
 * "zip list" datasets (zipEntry -> binary/url) which this writer writes. 
 * Datasets will be searched for a Binary variable first, then a "url" variable, 
 * then a "file" one. The first variable found will be used. The Operations deal 
 * with the different logic for each type.
 * 
 * Duplicate zip entries will be disambiguated by appending
 * a counter with an underscore (e.g. "_1"). TODO: disambiguate with domain values.
 */
class ZipWriter3 extends Writer with LazyLogging {
  //TODO: add manifest
  //TODO: add disambiguating "_1" before file extension

  def write(dataset: Dataset): Unit = {
    val zds: Dataset = toZipListDs(dataset)

    val zip = new ZipOutputStream(getOutputStream)
    try {
      zds match {
        case DatasetSamples(it) => it foreach {
          case Sample(Text(zipEntry), Binary(bytes)) =>
            zipFromBytes(zip, disambiguate(zipEntry), bytes)
          case Sample(Text(zipEntry), Text(url)) =>
            zipFromUrl(zip, disambiguate(zipEntry), url)
          case _ => throw new RuntimeException("Not a valid zip dataset")
        }
        case _ => throw new RuntimeException("Empty dataset")
      }
      //TODO: add manifest file
    } finally {
      zip.close
    }
  }

  /**
   * Translate to "zip list" dataset based on binary vs url vs file variable.
   */
  def toZipListDs(ds: Dataset): Dataset = 
    if (findBinaryVariable(ds).isDefined)
      new BinaryListToZipList()(ds)
    else if (ds.findVariableByName("url").nonEmpty)
      new UrlListToZipList()(ds)
    else if (ds.findVariableByName("file").nonEmpty)
      new FileListToZipList()(ds)
    else throw new RuntimeException("No 'url', 'file', nor binary variable found in dataset.")
  

  /**
   * Adds an entry to the ZipOutputStream with the given bytes.
   */
  def zipFromBytes(zip: ZipOutputStream, name: String, bytes: Array[Byte]): Unit = {
    try {
      zip.putNextEntry(new ZipEntry(name))
      zip.write(bytes)
    } catch {
      case e: IOException =>
        // Log a warning, leave empty entry 
        val msg = "Failed to write bytes to the zip entry."
        logger.warn(msg, e)
    }
    zip.closeEntry //finalize the zip entry
  }

  /**
   * Adds an entry to the ZipOutputStream by reading the bytes from the url.
   */
  def zipFromUrl(zip: ZipOutputStream, name: String, url: String): Unit = {
    // Open the URL input stream
    val bis: InputStream = try {
      new BufferedInputStream(new URL(url).openStream())
    } catch {
      case e: Exception =>
        val msg = s"Failed to open the URL: $url"
        throw new RuntimeException(msg, e)
    }

    // Write the zip entry
    try {
      zip.putNextEntry(new ZipEntry(name))
      Stream.continually(bis.read).takeWhile(_ != -1).foreach(zip.write(_))
    } catch {
      case e: IOException =>
        // Log a warning, leave empty entry //TODO: add error message to entry?
        val msg = s"Failed to read the URL: $url"
        logger.warn(msg, e)
    } finally {
      bis.close //make sure we release the file resource
    }
    zip.closeEntry //finalize the zip entry
  }

  /**
   * Returns the first binary variable found in the dataset, if there is one.
   */
  private def findBinaryVariable(ds: Dataset): Option[Variable] = ds.getScalars.find {
    case Binary(_) => true
    case _ => false
  }

  /**
   * Keeps a counter so we can disambiguate duplicate zip entry names.
   * We do it this way so we can stream samples.
   */
  private val counter = scala.collection.mutable.Map[String, Int]() //TODO: will need to be Map[String, String]

  /**
   * Makes sure that we don't duplicate zip entry names.
   * This will append "_d" for names that have already been used
   * where "d" is the stringified form of the given domain value.
   * 
   * TODO: Can't access domain variable because we get rid of it in *ListToZipList ops. 
   *       Will have to refactor "zip list" datasets to: domain -> (zipEntry, serializable).
   */
  private def disambiguate(name: String/*, domain: Variable*/): String = counter.get(name) match {
    case Some(n) =>
      counter += ((name, n + 1))
      name + "_" + n
    case None =>
      counter += ((name, 1))
      name
  }

  /**
   * Returns a String representation of a domain variable
   * that is acceptable as a zip entry name.
   */
  private def domainToString(domain: Variable): String = domain match {
    case time: Time => time match {
      case Text(t) => t //keep formatting
      case _: Number => time.toIso //TODO: consider stripping special characters like '-' and ':'
    }
    case tup: Tuple => tup.getVariables.map(domainToString(_)).mkString("_")
    case _ => domain.toString
  }

  override def mimeType: String = "application/zip"
}

object ZipWriter3 {

  def apply(out: OutputStream) = {
    val w = new ZipWriter3()
    w.setOutputStream(out)
    w
  }
}
