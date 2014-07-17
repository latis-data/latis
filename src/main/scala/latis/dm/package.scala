package latis.dm

import scala.language.implicitConversions

package object implicits {
  
  /**
   * Treat a Variable as a Dataset.
   * If it IS-A Dataset, "cast" it,
   * otherwise wrap it in a Dataset.
   */
  implicit def variableToDataset(v: Variable): Dataset = v match {
    case ds: Dataset => ds
    case _ => Dataset(v) //TODO: metadata?
  }
  
  //implicit def doubleToReal(d: Double): Real = Real(d)
  //implicit def longToInteger(l: Long): Integer = Integer(l)
  //... 
  
  //implicit def doubleToData(d: Double): Data = DoubleValue(d)
  //...
  
  
  implicit def doubleToDataset(d: Double): Dataset = Dataset(Real(d))
  //implicit def intToDataset(i: Int): Dataset = Dataset(Integer(i))
}