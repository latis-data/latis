package latis.util

/**
 * A custom exception that is intended to be thrown when a requested dataset cannot be found. 
 */
final case class DatasetNotFoundException(
  private val message: String = "", 
  private val cause: Throwable = None.orNull
) extends RuntimeException(message, cause) {
  
}