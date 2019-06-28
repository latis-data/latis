package latis.util.iterator

import scala.annotation.tailrec
import com.typesafe.scalalogging.LazyLogging
import java.sql.SQLException
import java.io.IOException

/**
 * An Iterator that looks ahead and caches the next sample.
 * This makes it easier to know when we are done, especially 
 * when we are filtering. The original source may have another 
 * sample but this will keep looking for the next valid sample.
 * You can also "peek" at the next sample without advancing.
 * This implementation of the PeekIterator applies a function
 * mapping to each element as it iterates.
 */
class MappingIterator[S,T >: Null](iterator: Iterator[S], f: S => Option[T]) extends PeekIterator[T] with LazyLogging {
  //Note, the bound on Null allows us to return null for generic type T.
  //TODO: could we do it.flatMap(f)? but would no longer be a PeekIterator? unless we do CanBuildFrom...?

  if (iterator == null) throw new RuntimeException("Can't construct a MappingIterator with a null Iterator.")
  
  /**
   * Responsible for getting the next transformed item 
   * or null if there are no more valid items.
   * This will keep trying until a valid sample is found 
   * or it hits the end of the original iterator.
   */
  @tailrec final protected def getNext: T = {
    if (! iterator.hasNext) null
    else {
      //Apply the operation. Skip element if there is an Exception.
      val result = try {
        f(iterator.next)
      } catch {
        case iox @ (_: IOException | _: SQLException) =>
          //Assume that we lost our data stream and abort.
          //Throw an Error so this recursive method won't swallow it.
          val msg = "MappingIterator lost the data stream. Aborting."
          throw new Error(msg, iox)
        case e: Exception =>
          //Note, Log only the message at "warn" level to reduce noise in the logs.
          //Set log level to include "debug" to see the stack traces.
          logger.warn("Sample dropped. MappingIterator got Exception trying to get next sample: " + e.getMessage)
          logger.debug("Sample dropped. MappingIterator got Exception trying to get next sample.", e)
          None
      }
      
      result match {
        case Some(t) => t
        case None => getNext //invalid value, try another
      }
    }
  }
}
