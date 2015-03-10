package latis.writer

import scala.io.Source

/**
 * Represents a parsed template. Parsing simply involves
 * reading the template and constructing a data structure
 * that 
 */
class StringTemplate(tmplStr: String) {
  
  private val parsedTemplate: Array[TemplateChunk] = parseTemplate(tmplStr)
  
  /**
   * Either represents a raw string directly from the parsed template
   * that should not be changed, or a mapping that should be replaced
   * with its value from the context object.
   */
  private class TemplateChunk(val isLiteral: Boolean, val str: String) { }
  
  /**
   * Wraps a regex in another regex that uses lookahead/lookbehind to
   * capture delimiters as well as the usual strings in between the
   * delimiters (intended to be used with String.split)
   * 
   * Adapted from: http://stackoverflow.com/questions/2206378/how-to-split-a-string-but-also-keep-the-delimiters
   */
  private def withDelimiter(s: String): String = s"((?<=$s)|(?=$s))"
  
  /**
   * Parse a template string into TemplateChunks
   */
  private def parseTemplate(tmplStr: String): Array[TemplateChunk] = {
    val chunks = tmplStr.split(withDelimiter(StringTemplate.keyRegex))
    chunks.map(chunk => {
      val matches = chunk.matches(StringTemplate.keyRegex)
      val str = if (matches) chunk.substring(2, chunk.length-2) else chunk
      new TemplateChunk(!matches, str)
    })
  }
  
  /**
   * Apply the passed values to the internal datastructure to
   * create a string that is an instantiation of this template
   * with those values
   */
  def applyValues(values: Map[String, String]): String = {
    parsedTemplate.
      map(chunk => if (chunk.isLiteral) chunk.str else values.getOrElse(chunk.str, throw new Exception(s"""template context missing value: "$chunk.str""""))).
      mkString
  }
}

object StringTemplate {
  
  val MAX_NAME_SIZE = 32
  private val keyRegex = s"\\{\\{[^}]{1,$MAX_NAME_SIZE}\\}\\}"
  
  def apply(tmplStr: String): StringTemplate = new StringTemplate(tmplStr)
  
  def fromResource(resourcePath: String): StringTemplate = StringTemplate(readResource(resourcePath))
  
  def readResource(resourcePath: String): String = {
    val rsrc = getClass.getResource(if (resourcePath.startsWith("/")) resourcePath else "/" + resourcePath)
    val chunks = Source.fromURL(rsrc)
    val result = chunks.mkString
    chunks.close
    result
  }
}