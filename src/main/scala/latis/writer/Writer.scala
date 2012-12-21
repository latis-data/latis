package latis.writer

import latis.dm.Dataset

/**
 * Base class for Dataset writers.
 */
abstract class Writer {

  /**
   * Output the given Dataset in the desired form.
   */
  def write(dataset: Dataset)
  
  /**
   * Release resources acquired by this Writer.
   */
  def close()
  
}