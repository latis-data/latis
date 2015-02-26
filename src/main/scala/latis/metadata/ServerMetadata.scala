package latis.metadata

import latis.writer.Writer
import latis.util.LatisProperties

/**
 * Static class that gathers metadata about the currently
 * running LaTiS instance. For example, it can tell you
 * about the various suffixes that have registered Writers
 * (like "csv" -> CsvWriter)
 */
object ServerMetadata {
  
  /**
   * Return a mapping of available suffixes to
   * WriterDescription objects that contain information about
   * them.
   */
  lazy val availableSuffixes: Seq[WriterDescription] = {
    val writerClassRegex = """writer\.(\w+)\.class""".r
    LatisProperties.keys.
      flatMap(key => key match {
        case writerClassRegex(suffix) => {
          Some(WriterDescription(
            suffix,
            LatisProperties.getOrElse(key, "[Unknown Class]").split("\\.").last,
            LatisProperties.getOrElse(key.replace(".class", ".description"), s"[No Description: add writer.$suffix.description to latis.properties]")
          ))
        }
        case _ => None
      }).
      toSeq
  }
}