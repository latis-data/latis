package latis.metadata

/**
 * Contains (usually human-readable) information about a
 * Writer class. Used primarily for describing the available
 * suffixes on the Overview page (OverviewWriter).
 */
class WriterDescription(val suffix: String, val className: String, val description: String) { }

object WriterDescription {
  def apply(suffix: String, className: String, description: String):WriterDescription = {
    new WriterDescription(suffix, className, description)
  }
}