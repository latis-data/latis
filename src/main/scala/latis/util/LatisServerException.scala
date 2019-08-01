package latis.util

/**
 * A custom exception that is intended to be thrown when service user errors occur. 
 */
final case class LatisServerException(
  private val message: String = "", 
  private val cause: Throwable = None.orNull
) extends RuntimeException(message, cause) {
  
}