package latis.util

import scala.reflect.runtime.currentMirror

/**
 * Collection of utility methods for using reflection.
 */
object ReflectionUtils {
  
  /**
   * Construct a class given its fully resolved name.
   * Assumes there are no constructor arguments.
   */
  def constructClassByName(className: String): Any = {
    val cls = Class.forName(className) //TODO: handle ClassNotFoundException?
    cls.getConstructor().newInstance()
  }
}