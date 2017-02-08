package latis.time

class TimeUnit(val seconds: Double, val name: String) {
    
  override def toString = name
}

object TimeUnit {
  //TODO: look at definitions from other time APIs that we could reuse
  //TODO: implement as case class or other form of enumeration?
  
  // Keep a map of units so we can access them by name, as well as identifier.
  private val units = new scala.collection.mutable.LinkedHashMap[String, TimeUnit]()
    
  val ATTOSECOND  = new TimeUnit(1e-18, "attoseconds")
  units += ((ATTOSECOND.name, ATTOSECOND))
  
  val FEMTOSECOND = new TimeUnit(1e-15, "femtoseconds")
  units += ((FEMTOSECOND.name, FEMTOSECOND))
  
  val PICOSECOND  = new TimeUnit(1e-12, "picoseconds")
  units += ((PICOSECOND.name, PICOSECOND))
  
  val NANOSECOND  = new TimeUnit(1e-9,  "nanoseconds")
  units += ((NANOSECOND.name, NANOSECOND))
  
  val MICROSECOND = new TimeUnit(1e-6,  "microseconds")
  units += ((MICROSECOND.name, MICROSECOND))
  
  val MILLISECOND = new TimeUnit(0.001, "milliseconds")
  units += ((MILLISECOND.name, MILLISECOND))
  
  val SECOND      = new TimeUnit(1,     "seconds")
  units += ((SECOND.name, SECOND))
  
  val MINUTE      = new TimeUnit(60,    "minutes")
  units += ((MINUTE.name, MINUTE))
  
  val HOUR        = new TimeUnit(3600,  "hours")
  units += ((HOUR.name, HOUR))
  
  // NOTE: Day is defined as exactly 86400 SI seconds.
  val DAY         = new TimeUnit(86400, "days")
  units += ((DAY.name, DAY))
  
  val WEEK        = new TimeUnit(DAY.seconds * 7,     "weeks")
  units += ((WEEK.name, WEEK))
  
  //TODO: include MONTH (as 30 days?) even though it isn't consistent?
  
  val YEAR        = new TimeUnit(DAY.seconds * 365,   "years")
  //TODO: should this be actual year? obs vs model
  units += ((YEAR.name, YEAR))
  
  val DECADE      = new TimeUnit(YEAR.seconds * 10,   "decades")
  units += ((DECADE.name, DECADE))
  
  val CENTURY     = new TimeUnit(YEAR.seconds * 100,  "centuries")
  units += ((CENTURY.name, CENTURY))
  
  val MILLENNIUM  = new TimeUnit(YEAR.seconds * 1000, "millennia")
  units += ((MILLENNIUM.name, MILLENNIUM))
  
  val FORTNIGHT   = new TimeUnit(DAY.seconds * 14, "fortnights")
  units += ((FORTNIGHT.name, FORTNIGHT))

  
  /**
   * Get the TimeUnit instance by name.
   */
  def withName(name: String): TimeUnit = units(name.toLowerCase)

}